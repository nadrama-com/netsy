// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSFiles struct {
	ServerCert *tls.Certificate
	ServerCA   *x509.CertPool
	ClientCert *tls.Certificate
	ClientCA   *x509.CertPool
}

func LoadTLSFiles(c *Config) (*TLSFiles, error) {
	// Load server and client certificates and keys
	serverCert, err := tls.LoadX509KeyPair(c.TLSServerCert(), c.TLSServerKey())
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert %s and key %s: %w", c.TLSServerCert(), c.TLSServerKey(), err)
	}
	clientCert, err := tls.LoadX509KeyPair(c.TLSClientCert(), c.TLSClientKey())
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert %s and key %s: %w", c.TLSClientCert(), c.TLSClientKey(), err)
	}
	// Load CA pools - note that these are probably the same
	serverCA := x509.NewCertPool()
	serverCAPem, err := os.ReadFile(c.TLSServerCA())
	if err != nil {
		return nil, fmt.Errorf("failed to read server CA file: %w", err)
	}
	if !serverCA.AppendCertsFromPEM(serverCAPem) {
		return nil, fmt.Errorf("failed to append server CA cert to pool: %w", err)
	}
	var clientCA *x509.CertPool
	if c.TLSServerCA() == c.TLSClientCA() {
		clientCA = serverCA
	} else {
		clientCA = x509.NewCertPool()
		clientCAPem, err := os.ReadFile(c.TLSClientCA())
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA file: %w", err)
		}
		if !clientCA.AppendCertsFromPEM(clientCAPem) {
			return nil, fmt.Errorf("failed to append client CA cert to pool: %w", err)
		}
	}
	return &TLSFiles{
		ServerCA:   serverCA,
		ServerCert: &serverCert,
		ClientCA:   clientCA,
		ClientCert: &clientCert,
	}, nil
}
