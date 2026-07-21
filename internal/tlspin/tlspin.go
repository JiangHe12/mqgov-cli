package tlspin

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/trust"
)

const (
	Algorithm = "tls-spki-sha256"
	FileName  = "tls_known_hosts"
)

type Event struct {
	Address     string
	Algorithm   string
	Fingerprint string
}

type NotifyFunc func(Event)

func DefaultPath() (string, error) {
	dir, err := corectx.ConfigDir()
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve TLS trust directory", err)
	}
	return filepath.Join(dir, FileName), nil
}

func Attach(cfg *tls.Config, pinPath string, notify NotifyFunc) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}
	return attach(cfg, pinPath, "", notify)
}

func CloneForEndpoint(cfg *tls.Config, pinPath, endpoint string, notify NotifyFunc) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}
	address, serverName := endpointIdentity(endpoint)
	if address == "" || serverName == "" {
		return nil, apperrors.New(apperrors.CodeValidationFailed, "TLS certificate pinning requires a non-empty endpoint", nil)
	}
	cloned := cfg.Clone()
	cloned.ServerName = serverName
	return attach(cloned, pinPath, address, notify)
}

func attach(cfg *tls.Config, pinPath, address string, notify NotifyFunc) (*tls.Config, error) {
	path := pinPath
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, err
		}
	}
	cfg.InsecureSkipVerify = false
	previous := cfg.VerifyConnection
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if previous != nil {
			if err := previous(cs); err != nil {
				return err
			}
		}
		if address != "" {
			return VerifyConnectionForAddress(path, address, cs, notify)
		}
		return VerifyConnection(path, cs, notify)
	}
	return cfg, nil
}

func VerifyConnection(pinPath string, cs tls.ConnectionState, notify NotifyFunc) error {
	address := strings.TrimSpace(cs.ServerName)
	if address == "" {
		return apperrors.New(apperrors.CodeValidationFailed, "TLS certificate pinning requires a non-empty server name", nil)
	}
	return VerifyConnectionForAddress(pinPath, address, cs, notify)
}

func VerifyConnectionForAddress(pinPath, address string, cs tls.ConnectionState, notify NotifyFunc) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return apperrors.New(apperrors.CodeValidationFailed, "TLS certificate pinning requires a non-empty endpoint", nil)
	}
	if len(cs.PeerCertificates) == 0 {
		return apperrors.New(apperrors.CodeAuthFailed, "TLS certificate pinning failed: server presented no certificates", nil)
	}
	material := cs.PeerCertificates[0].RawSubjectPublicKeyInfo
	pin := trust.Pin{
		Address:     address,
		Algorithm:   Algorithm,
		Fingerprint: Fingerprint(material),
		Material:    material,
	}
	var adapter func(trust.Pin)
	if notify != nil {
		adapter = func(pin trust.Pin) {
			notify(Event{Address: pin.Address, Algorithm: pin.Algorithm, Fingerprint: pin.Fingerprint})
		}
	}
	return translateTrustError(trust.New(pinPath).VerifyOrPin(address, pin, adapter))
}

func Fingerprint(material []byte) string {
	sum := sha256.Sum256(material)
	return "SHA256:" + base64.StdEncoding.EncodeToString(sum[:])
}

func NotifyStderr(event Event) {
	_, _ = fmt.Fprintf(os.Stderr, "TOFU: pinned TLS certificate for %s (%s %s)\n", event.Address, event.Algorithm, event.Fingerprint)
}

func AppError(err error) *apperrors.AppError {
	var appErr *apperrors.AppError
	if errors.As(err, &appErr) && isTLSTrustMessage(appErr.Message) {
		return appErr
	}
	return nil
}

func translateTrustError(err error) error {
	if err == nil {
		return nil
	}
	var changed *trust.PinChangedError
	if errors.As(err, &changed) {
		return apperrors.New(
			apperrors.CodeAuthFailed,
			fmt.Sprintf("TLS certificate pin changed for %s (%s): expected %s, received %s", changed.Address, changed.Algorithm, changed.ExpectedFingerprint, changed.ActualFingerprint),
			nil,
		)
	}
	var changedAlgorithm *trust.PinAlgorithmChangedError
	if errors.As(err, &changedAlgorithm) {
		return apperrors.New(
			apperrors.CodeAuthFailed,
			fmt.Sprintf("TLS certificate pin algorithm changed for %s: pinned algorithms %s, received %s", changedAlgorithm.Address, strings.Join(changedAlgorithm.PinnedAlgorithms, ", "), changedAlgorithm.ActualAlgorithm),
			nil,
		)
	}
	var appErr *apperrors.AppError
	if errors.As(err, &appErr) {
		if message, ok := tlsTrustErrorMessage(appErr.Message); ok {
			return apperrors.New(appErr.Code, message, appErr.Unwrap()).WithSuggestion(appErr.Suggestion)
		}
	}
	return err
}

func tlsTrustErrorMessage(message string) (string, bool) {
	switch message {
	case "failed to create trust directory":
		return "failed to create TLS trust directory", true
	case "failed to open trust pins":
		return "failed to open TLS certificate pins", true
	case "failed to parse trust pin":
		return "failed to parse TLS certificate pin", true
	case "invalid trust pin record":
		return "invalid TLS certificate pin record", true
	case "failed to read trust pins":
		return "failed to read TLS certificate pins", true
	case "failed to secure trust pins":
		return "failed to secure TLS certificate pins", true
	case "failed to write trust pin":
		return "failed to write TLS certificate pin", true
	case "failed to inspect trust pins":
		return "failed to inspect TLS certificate pins", true
	case "trust pin path must be a regular file":
		return "TLS certificate pin path must be a regular file", true
	case "trust pin permissions are insecure":
		return "TLS certificate pin permissions are insecure", true
	default:
		return "", false
	}
}

func isTLSTrustMessage(message string) bool {
	return strings.Contains(message, "TLS certificate pin") || strings.Contains(message, "TLS trust")
}

func endpointIdentity(endpoint string) (address, serverName string) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", ""
	}
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		return hostIdentity(firstEndpointHost(parsed.Host))
	}
	return hostIdentity(firstEndpointHost(endpoint))
}

func firstEndpointHost(host string) string {
	host = strings.TrimSpace(host)
	if comma := strings.IndexByte(host, ','); comma >= 0 {
		host = host[:comma]
	}
	return strings.TrimSpace(host)
}

func hostIdentity(host string) (address, serverName string) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", ""
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		return host, strings.Trim(parsedHost, "[]")
	}
	return host, strings.Trim(host, "[]")
}
