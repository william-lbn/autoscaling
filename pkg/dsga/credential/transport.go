package credential

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
)

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, failure("trusted_file_unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(data) == 0 || int64(len(data)) > maximum {
		return nil, failure("invalid_trusted_file")
	}
	return data, nil
}

// 管理CA只从可信Agent配置指定的投影文件读取。允许K8s投影目录通过symlink原子
// 轮转，不把其当异常；只接受CA CERTIFICATE块，拒绝混入私钥或普通Guest叶证书。
// 时间/完整链由每次标准TLS握手验证，允许新旧CA重叠，而不是因旧根过期拒绝新链。
func managementRoots(path string) (*x509.CertPool, error) {
	data, err := readBoundedFile(path, 64*1024)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	count := 0
	for len(strings.TrimSpace(string(data))) != 0 {
		data = bytes.TrimSpace(data)
		if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, failure("invalid_management_ca")
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, failure("invalid_management_ca")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, failure("invalid_management_ca")
		}
		pool.AddCert(certificate)
		count++
		if count > 32 {
			return nil, failure("management_ca_limit")
		}
		data = rest
	}
	if count == 0 {
		return nil, failure("invalid_management_ca")
	}
	return pool, nil
}

// ManagementTransport为凭据/Compute各连接池创建独立传输。固定目标URL/SNI不由
// 业务请求提供；不采用环境proxy，不禁用标准链/主机名验证。每次新TLS连接读取
// 当前管理CA文件，轮转不重造SDK或清Pending。已建立连接遵守正常TLS建连语义。
func (client *Client) ManagementTransport() *http.Transport {
	transport := new(http.Transport)
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := client.callContext(ctx)
		defer cancel()
		if network != "tcp" || address != client.address {
			return nil, failure("foreign_management_target")
		}
		roots, err := managementRoots(client.config.CAFile)
		if err != nil {
			return nil, err
		}
		config := &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: client.serverName,
			VerifyConnection: func(state tls.ConnectionState) error {
				// 标准验证完成后仍须确认专用管理域身份。即使误配置Guest CA，
				// 也不能将Agent SA令牌发送给持有旧单Compute叶私钥的Guest。
				if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || state.PeerCertificates[0].IsCA {
					return failure("unverified_management_peer")
				}
				for _, name := range state.PeerCertificates[0].DNSNames {
					if name == "*."+client.proof.Namespace+".svc" {
						return nil
					}
				}
				return failure("guest_tls_identity_rejected")
			},
		}
		connection, err := (&tls.Dialer{NetDialer: &net.Dialer{Timeout: client.config.Timeout}, Config: config}).DialContext(ctx, network, address)
		if err != nil {
			return nil, failure("management_tls_unavailable")
		}
		return connection, nil
	}
	return transport
}
