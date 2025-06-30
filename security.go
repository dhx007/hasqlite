package hasqlite

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type SecurityConfig struct {
	EnableTLS    bool   `json:"enable_tls"`
	CertFile     string `json:"cert_file"`
	KeyFile      string `json:"key_file"`
	CAFile       string `json:"ca_file"`
	APIKey       string `json:"api_key"`
	EncryptData  bool   `json:"encrypt_data"`
	EncryptionKey string `json:"encryption_key"`
}

func (sc *SecurityConfig) LoadTLSConfig() (*tls.Config, error) {
	if !sc.EnableTLS {
		return nil, nil
	}
	
	cert, err := tls.LoadX509KeyPair(sc.CertFile, sc.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %v", err)
	}
	
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	
	if sc.CAFile != "" {
		caCert, err := ioutil.ReadFile(sc.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %v", err)
		}
		
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	
	return tlsConfig, nil
}

// 在APIServer中添加认证中间件
func (s *APIServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" || apiKey != s.config.Security.APIKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}