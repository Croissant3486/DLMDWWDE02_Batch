$pathToCerts = "INSERT\YOUR\PATH\HERE\certs"

$rootCert = New-SelfSignedCertificate -CertStoreLocation Cert:\LocalMachine\My -Subject "CN=RootCA" -KeyExportPolicy Exportable -KeySpec Signature -KeyLength 2048 -HashAlgorithm sha256 -Provider "Microsoft Enhanced RSA and AES Cryptographic Provider"

Export-Certificate -Cert $rootCert -FilePath "$pathToCerts\rootCA.cer"


$kafkaCert = New-SelfSignedCertificate -CertStoreLocation "$pathToCerts\" -DnsName "kafka" -Signer $rootCert -KeyExportPolicy Exportable -KeySpec Signature -KeyLength 2048 -HashAlgorithm sha256 -Provider "Microsoft Enhanced RSA and AES Cryptographic Provider"

$password = ConvertTo-SecureString -String "123QWEasd" -Force -AsPlainText
Export-PfxCertificate -Cert $kafkaCert -FilePath "$pathToCerts\kafka.pfx" -Password $password


$sparkCert = New-SelfSignedCertificate -CertStoreLocation "$pathToCerts\" -DnsName "spark" -Signer $rootCert -KeyExportPolicy Exportable -KeySpec Signature -KeyLength 2048 -HashAlgorithm sha256 -Provider "Microsoft Enhanced RSA and AES Cryptographic Provider"

$password = ConvertTo-SecureString -String "123QWEasd" -Force -AsPlainText
Export-PfxCertificate -Cert $sparkCert -FilePath "$pathToCerts\spark.pfx" -Password $password


$hdfsCert = New-SelfSignedCertificate -CertStoreLocation "$pathToCerts\" -DnsName "hdfs" -Signer $rootCert -KeyExportPolicy Exportable -KeySpec Signature -KeyLength 2048 -HashAlgorithm sha256 -Provider "Microsoft Enhanced RSA and AES Cryptographic Provider"

$password = ConvertTo-SecureString -String "123QWEasd" -Force -AsPlainText
Export-PfxCertificate -Cert $hdfsCert -FilePath "$pathToCerts\hdfs.pfx" -Password $password

# Import the certificate (.cer) into the local machine's store
$certPath = "$pathToCerts\rootCA.cer"
$cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2
$cert.Import($certPath)

# Export the certificate to a .pfx file
$pfxPath = "$pathToCerts\rootCA.pfx"
$password = ConvertTo-SecureString -String "123QWEasd" -Force -AsPlainText
$cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pkcs12, $password) | Set-Content -Path $pfxPath -Encoding Byte