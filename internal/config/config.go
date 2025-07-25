// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/spf13/viper"
)

// Config provides getters/setters for working with the config
type Config struct {
	logger log.Logger
}

// Init initializes the Config struct
func Init(logger log.Logger) (*Config, error) {
	c := &Config{logger: logger}
	return c, nil
}

// runtimeConfig defines the config variables, validation, and viper config
// TODO: add path to leaders list file
type runtimeConfig struct {
	Environment       string `viper:"environment" envkey:"ENVIRONMENT" default:"development" description:"Environment (development|production|[string])"`
	InstanceID        string `viper:"instance_id" validate:"puidv7" envkey:"INSTANCE_ID" default:"" description:"Random puidv7 of this instance"`
	InstanceHostname  string `viper:"instance_hostname" validate:"hostname" envkey:"INSTANCE_HOSTNAME" default:"" description:"Hostname of this instance"`
	Verbose           bool   `viper:"verbose" envkey:"NETSY_DEBUG" default:"false" description:"Enable verbose output"`
	ListenClientsAddr string `viper:"listen_clients_addr" envkey:"NETSY_LISTEN_CLIENTS_ADDR" default:":2378" description:"Address of etcd-compatible API server for client requests"`
	ListenPeersAddr   string `viper:"listen_peers_addr" envkey:"NETSY_LISTEN_PEERS_ADDR" default:":2381" description:"Address for other netsy servers to connect to"`
	TLSServerCA       string `viper:"tls_server_ca" envkey:"NETSY_TLS_SERVER_CA" default:"" description:"Path to file containing the CA x509 certificate used when serving connections on the server listen address"`
	TLSServerCert     string `viper:"tls_server_cert" envkey:"NETSY_TLS_SERVER_CERT" default:"" description:"Path to file containing the x509 certificate used when serving connections on the server listen address"`
	TLSServerKey      string `viper:"tls_server_key" envkey:"NETSY_TLS_SERVER_KEY" default:"" description:"Path to file containing the Ed25519 private key used when serving connections on the server listen address"`
	TLSClientCA       string `viper:"tls_client_ca" envkey:"NETSY_TLS_CLIENT_CA" default:"" description:"Path to file containing the CA x509 certificate used when connecting to peer netsy servers"`
	TLSClientCert     string `viper:"tls_client_cert" envkey:"NETSY_TLS_CLIENT_CERT" default:"" description:"Path to file containing the x509 certificate used when connecting to peer netsy servers"`
	TLSClientKey      string `viper:"tls_client_key" envkey:"NETSY_TLS_CLIENT_KEY" default:"" description:"Path to file containing the Ed25519 private key used when connecting to peer netsy servers"`
	DataDir           string `viper:"data_dir" validate:"omitempty,dirpath" envkey:"NETSY_DATA_DIR" default:"/opt/data" description:"(Optional) Path to directory for data"`
	// S3 Configuration
	S3Enabled         bool   `viper:"s3_enabled" envkey:"NETSY_S3_ENABLED" default:"true" description:"Enable S3 storage backend (default = true)"`
	S3BucketName      string `viper:"s3_bucket_name" validate:"required_if=S3Enabled true" envkey:"NETSY_S3_BUCKET_NAME" default:"" description:"S3 bucket name (required when S3 is enabled)"`
	S3KeyPrefix       string `viper:"s3_key_prefix" envkey:"NETSY_S3_KEY_PREFIX" default:"" description:"S3 object key prefix"`
	S3Region          string `viper:"s3_region" envkey:"AWS_DEFAULT_REGION" default:"us-east-1" description:"AWS region for S3 bucket"`
	S3Endpoint        string `viper:"s3_endpoint" envkey:"AWS_ENDPOINT_URL" default:"" description:"Custom S3 endpoint URL (for MinIO, etc.)"`
	S3AccessKeyID     string `viper:"s3_access_key_id" envkey:"AWS_ACCESS_KEY_ID" default:"" description:"AWS access key ID (optional, prefer IAM roles)"`
	S3SecretAccessKey string `viper:"s3_secret_access_key" envkey:"AWS_SECRET_ACCESS_KEY" default:"" description:"AWS secret access key (optional, prefer IAM roles)"`
	S3SessionToken    string `viper:"s3_session_token" envkey:"AWS_SESSION_TOKEN" default:"" description:"AWS session token for temporary credentials"`
	S3RoleArn         string `viper:"s3_role_arn" envkey:"NETSY_S3_ROLE_ARN" default:"" description:"IAM role ARN to assume for S3 access"`
	S3RoleSessionName string `viper:"s3_role_session_name" envkey:"NETSY_S3_ROLE_SESSION_NAME" default:"netsy-session" description:"Session name when assuming IAM role"`
	S3ForcePathStyle  bool   `viper:"s3_force_path_style" envkey:"NETSY_S3_FORCE_PATH_STYLE" default:"false" description:"Use path-style S3 addressing (required for MinIO)"`
	S3StorageClass    string `viper:"s3_storage_class" envkey:"NETSY_S3_STORAGE_CLASS" default:"STANDARD" description:"S3 storage class (STANDARD, STANDARD_IA, GLACIER, etc.)"`
	S3Encryption      string `viper:"s3_encryption" envkey:"NETSY_S3_ENCRYPTION" default:"AES256" description:"S3 server-side encryption (AES256 or aws:kms)"`
	S3KMSKeyID        string `viper:"s3_kms_key_id" envkey:"NETSY_S3_KMS_KEY_ID" default:"" description:"KMS key ID for S3 encryption (when using aws:kms)"`
	// Replication Configuration
	ReplicationMode string `viper:"replication_mode" envkey:"NETSY_REPLICATION_MODE" default:"synchronous" description:"Replication mode (synchronous|asynchronous)"`
	// Snapshot Configuration
	SnapshotThresholdRecords    int64 `viper:"snapshot_threshold_records" envkey:"NETSY_SNAPSHOT_THRESHOLD_RECORDS" default:"10000" description:"Create snapshot after N records since last snapshot (0 = disabled)"`
	SnapshotThresholdSizeMB     int64 `viper:"snapshot_threshold_size_mb" envkey:"NETSY_SNAPSHOT_THRESHOLD_SIZE_MB" default:"10000" description:"Create snapshot when chunks exceed N MB (0 = disabled)"`
	SnapshotThresholdAgeMinutes int64 `viper:"snapshot_threshold_age_minutes" envkey:"NETSY_SNAPSHOT_THRESHOLD_AGE_MINUTES" default:"0" description:"Create snapshot after N minutes since last snapshot (0 = disabled)"`
}

// Environment returns the current environment (development, production, etc)
func (c *Config) Environment() string {
	return viper.GetString("environment")
}

// InstanceID returns the ID of the current instance
func (c *Config) InstanceID() string {
	return viper.GetString("instance_id")
}

// InstanceHostname returns the hostname of the current instance
func (c *Config) InstanceHostname() string {
	return viper.GetString("instance_hostname")
}

// Verbose returns whether verbose mode is enabled
func (c *Config) Verbose() bool {
	return viper.GetBool("verbose")
}

// ListenClientsAddr returns the address of etcd-compatible API server for client requests
func (c *Config) ListenClientsAddr() string {
	return viper.GetString("listen_clients_addr")
}

// ListenPeersAddr returns the address for other netsy servers to connect to
func (c *Config) ListenPeersAddr() string {
	return viper.GetString("listen_peers_addr")
}

// TLSServerCA returns the path to file containing the CA x509 certificate used when serving connections on the server listen address
func (c *Config) TLSServerCA() string {
	caCert := viper.GetString("tls_server_ca")
	if strings.HasPrefix(caCert, "./") {
		caCert = strings.TrimPrefix(caCert, "./")
		currentDir, _ := filepath.Abs(".")
		caCert = filepath.Join(currentDir, caCert)
		viper.Set("tls_server_ca", caCert)
	}
	return caCert
}

// TLSServerCert returns the path to file containing the x509 certificate used when serving connections on the server listen address
func (c *Config) TLSServerCert() string {
	serverCert := viper.GetString("tls_server_cert")
	if strings.HasPrefix(serverCert, "./") {
		serverCert = strings.TrimPrefix(serverCert, "./")
		currentDir, _ := filepath.Abs(".")
		serverCert = filepath.Join(currentDir, serverCert)
		viper.Set("tls_server_cert", serverCert)
	}
	return serverCert
}

// TLSServerKey returns the path to the Path to file containing the Ed25519 private key used when serving connections on the server listen address
func (c *Config) TLSServerKey() string {
	keyFile := viper.GetString("tls_server_key")
	if strings.HasPrefix(keyFile, "./") {
		keyFile = strings.TrimPrefix(keyFile, "./")
		currentDir, _ := filepath.Abs(".")
		keyFile = filepath.Join(currentDir, keyFile)
		viper.Set("tls_server_key", keyFile)
	}
	return keyFile
}

// TLSClientCA returns the path to file containing the CA x509 certificate used when connecting to peer netsy servers
func (c *Config) TLSClientCA() string {
	caCert := viper.GetString("tls_client_ca")
	if strings.HasPrefix(caCert, "./") {
		caCert = strings.TrimPrefix(caCert, "./")
		currentDir, _ := filepath.Abs(".")
		caCert = filepath.Join(currentDir, caCert)
		viper.Set("tls_client_ca", caCert)
	}
	return caCert
}

// TLSClientCert returns the path to file containing the x509 certificate used when connecting to peer netsy servers
func (c *Config) TLSClientCert() string {
	clientCert := viper.GetString("tls_client_cert")
	if strings.HasPrefix(clientCert, "./") {
		clientCert = strings.TrimPrefix(clientCert, "./")
		currentDir, _ := filepath.Abs(".")
		clientCert = filepath.Join(currentDir, clientCert)
		viper.Set("tls_client_cert", clientCert)
	}
	return clientCert
}

// TLSClientKey returns the path to file containing the Ed25519 private key used when connecting to peer netsy servers
func (c *Config) TLSClientKey() string {
	keyFile := viper.GetString("tls_client_key")
	if strings.HasPrefix(keyFile, "./") {
		keyFile = strings.TrimPrefix(keyFile, "./")
		currentDir, _ := filepath.Abs(".")
		keyFile = filepath.Join(currentDir, keyFile)
		viper.Set("tls_client_key", keyFile)
	}
	return keyFile
}

// DataDir returns the directory path for data
func (c *Config) DataDir() string {
	dir := viper.GetString("data_dir")
	if strings.HasPrefix(dir, "./") {
		dir = strings.TrimPrefix(dir, "./")
		currentDir, _ := filepath.Abs(".")
		dir = filepath.Join(currentDir, dir)
		viper.Set("data_dir", dir)
	}
	return dir
}

// S3Enabled returns whether S3 storage backend is enabled
func (c *Config) S3Enabled() bool {
	return viper.GetBool("s3_enabled")
}

// S3BucketName returns the S3 bucket name
func (c *Config) S3BucketName() string {
	return viper.GetString("s3_bucket_name")
}

// S3KeyPrefix returns the S3 object key prefix
func (c *Config) S3KeyPrefix() string {
	return viper.GetString("s3_key_prefix")
}

// S3Region returns the AWS region for S3 bucket
func (c *Config) S3Region() string {
	return viper.GetString("s3_region")
}

// S3Endpoint returns the custom S3 endpoint URL
func (c *Config) S3Endpoint() string {
	return viper.GetString("s3_endpoint")
}

// S3AccessKeyID returns the AWS access key ID
func (c *Config) S3AccessKeyID() string {
	return viper.GetString("s3_access_key_id")
}

// S3SecretAccessKey returns the AWS secret access key
func (c *Config) S3SecretAccessKey() string {
	return viper.GetString("s3_secret_access_key")
}

// S3SessionToken returns the AWS session token for temporary credentials
func (c *Config) S3SessionToken() string {
	return viper.GetString("s3_session_token")
}

// S3RoleArn returns the IAM role ARN to assume for S3 access
func (c *Config) S3RoleArn() string {
	return viper.GetString("s3_role_arn")
}

// S3RoleSessionName returns the session name when assuming IAM role
func (c *Config) S3RoleSessionName() string {
	return viper.GetString("s3_role_session_name")
}

// S3ForcePathStyle returns whether to use path-style S3 addressing
func (c *Config) S3ForcePathStyle() bool {
	return viper.GetBool("s3_force_path_style")
}

// S3StorageClass returns the S3 storage class
func (c *Config) S3StorageClass() string {
	return viper.GetString("s3_storage_class")
}

// S3Encryption returns the S3 server-side encryption type
func (c *Config) S3Encryption() string {
	return viper.GetString("s3_encryption")
}

// S3KMSKeyID returns the KMS key ID for S3 encryption
func (c *Config) S3KMSKeyID() string {
	return viper.GetString("s3_kms_key_id")
}

// ReplicationMode returns the replication mode (synchronous|asynchronous)
func (c *Config) ReplicationMode() string {
	return viper.GetString("replication_mode")
}

// SnapshotThresholdRecords returns the record count threshold for snapshots
func (c *Config) SnapshotThresholdRecords() int64 {
	return viper.GetInt64("snapshot_threshold_records")
}

// SnapshotThresholdSizeMB returns the size threshold in MB for snapshots
func (c *Config) SnapshotThresholdSizeMB() int64 {
	return viper.GetInt64("snapshot_threshold_size_mb")
}

// SnapshotThresholdAgeMinutes returns the age threshold in minutes for snapshots
func (c *Config) SnapshotThresholdAgeMinutes() int64 {
	return viper.GetInt64("snapshot_threshold_age_minutes")
}
