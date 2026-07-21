package tlspin

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
)

func TestVerifyConnectionPinsAndReusesSPKI(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	key := testKey(t)
	first := testCertificate(t, key)
	second := testCertificate(t, key)
	address := "broker.example:443"

	var events []Event
	if err := VerifyConnection(path, tls.ConnectionState{ServerName: address, PeerCertificates: []*x509.Certificate{first}}, func(event Event) {
		events = append(events, event)
	}); err != nil {
		t.Fatalf("first VerifyConnection() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("TOFU notifications = %d, want 1", len(events))
	}
	if events[0].Address != address || events[0].Algorithm != Algorithm {
		t.Fatalf("TOFU event = %#v", events[0])
	}

	if err := VerifyConnection(path, tls.ConnectionState{ServerName: address, PeerCertificates: []*x509.Certificate{second}}, func(event Event) {
		events = append(events, event)
	}); err != nil {
		t.Fatalf("same-SPKI VerifyConnection() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("TOFU notifications after reuse = %d, want 1", len(events))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	want := address + "\t" + Algorithm + "\t" + Fingerprint(first.RawSubjectPublicKeyInfo) + "\t" + base64.StdEncoding.EncodeToString(first.RawSubjectPublicKeyInfo) + "\n"
	if string(data) != want {
		t.Fatalf("pin file = %q, want %q", data, want)
	}
}

func TestVerifyConnectionRejectsChangedSPKI(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	address := "broker.example:443"
	first := testCertificate(t, testKey(t))
	changed := testCertificate(t, testKey(t))

	if err := VerifyConnection(path, tls.ConnectionState{ServerName: address, PeerCertificates: []*x509.Certificate{first}}, nil); err != nil {
		t.Fatalf("initial VerifyConnection() error = %v", err)
	}
	err := VerifyConnection(path, tls.ConnectionState{ServerName: address, PeerCertificates: []*x509.Certificate{changed}}, nil)
	if err == nil {
		t.Fatal("VerifyConnection() error = nil, want changed pin rejection")
	}
	appErr := apperrors.AsAppError(err)
	if appErr.Code != apperrors.CodeAuthFailed {
		t.Fatalf("error code = %s, want %s; err=%v", appErr.Code, apperrors.CodeAuthFailed, err)
	}
	if !strings.Contains(appErr.Message, "TLS certificate pin changed for "+address) {
		t.Fatalf("error message = %q", appErr.Message)
	}
	if AppError(err) == nil {
		t.Fatalf("AppError() did not recognize TLS pin error: %v", err)
	}
}

func TestVerifyConnectionRequiresServerName(t *testing.T) {
	err := VerifyConnection(filepath.Join(t.TempDir(), FileName), tls.ConnectionState{PeerCertificates: []*x509.Certificate{testCertificate(t, testKey(t))}}, nil)
	if err == nil {
		t.Fatal("VerifyConnection() error = nil, want fail-closed empty ServerName")
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("error code = %s, want %s", got, apperrors.CodeValidationFailed)
	}
}

func TestAttachPreservesPreviousVerifyConnection(t *testing.T) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	errPrevious := errors.New("previous verifier")
	cfg.VerifyConnection = func(tls.ConnectionState) error { return errPrevious }

	attached, err := Attach(cfg, filepath.Join(t.TempDir(), FileName), nil)
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if attached.InsecureSkipVerify {
		t.Fatal("Attach() enabled InsecureSkipVerify")
	}
	if err := attached.VerifyConnection(tls.ConnectionState{}); !errors.Is(err, errPrevious) {
		t.Fatalf("VerifyConnection() error = %v, want previous verifier", err)
	}
}

func TestCloneForEndpointUsesStableEndpointAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	cert := testCertificate(t, testKey(t))

	attached, err := CloneForEndpoint(cfg, path, "https://broker.example:8443/schemas", nil)
	if err != nil {
		t.Fatalf("CloneForEndpoint() error = %v", err)
	}
	if cfg.ServerName != "" {
		t.Fatalf("base config ServerName = %q, want unchanged", cfg.ServerName)
	}
	if attached.ServerName != "broker.example" {
		t.Fatalf("cloned ServerName = %q, want broker.example", attached.ServerName)
	}
	if err := attached.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}); err != nil {
		t.Fatalf("VerifyConnection() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.HasPrefix(string(data), "broker.example:8443\t"+Algorithm+"\t") {
		t.Fatalf("pin file = %q, want endpoint address with host:port", data)
	}
}

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	return key
}

func testCertificate(t *testing.T, key *rsa.PrivateKey) *x509.Certificate {
	t.Helper()
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("rand.Int() error = %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "broker.example"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate() error = %v", err)
	}
	return cert
}
