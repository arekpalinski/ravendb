using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using Google.Apis.Http;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Security;
using RachisTests;
using Raven.Client;
using Raven.Server.Utils;
using SlowTests.Client.Attachments;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public enum Mode
        {
            GenerateNew,
            LoadCaAndGenerateNewPk,
            LoadCaAndRenew
        }


        public static void Main(string[] args)
        {
            const bool with2Eku = true;
            const Mode mode = Mode.LoadCaAndRenew;

            const string suffix = with2Eku ? "2eku" : "1eku";


            byte[] selfSignedServerACertificate;
            byte[] selfSignedServerBCertificate;
            byte[] selfSignedServerCCertificate;
            byte[] caCertBytes = null;

            var basePath = Path.Combine(@"D:\workspace\issues\24730-Server_Auth_EKU\24742-windows\cluster-same-cert\04-self-signed", $"{suffix}-certs");

            if (Directory.Exists(basePath) == false)
                throw new InvalidOperationException("Create directory first");

            switch (mode)
            {
                case Mode.GenerateNew:
                    {
                        ((selfSignedServerACertificate, selfSignedServerBCertificate, selfSignedServerCCertificate), caCertBytes) =
                            CertificateUtils.CreateCaAndSelfSignedNodesCertificates($"RavenDB-24742-{suffix}", $"RavenDB-24742-{suffix}-ca", with2Eku: with2Eku);

                        break;
                    }
                case Mode.LoadCaAndGenerateNewPk:
                    {

                        var caPath = Path.Combine(basePath, "ca.pfx");

                        (selfSignedServerACertificate, selfSignedServerBCertificate, selfSignedServerCCertificate) =
                            CertificateUtils.LoadCaAndSelfSignedNodesCertificates(caPath, $"RavenDB-24742-{suffix}", $"RavenDB-24742-{suffix}-ca", with2Eku: with2Eku);

                        basePath = Path.Combine(basePath, "new-private-keys");

                        Directory.CreateDirectory(basePath);

                        break;
                    }
                case Mode.LoadCaAndRenew:
                    {
                        (selfSignedServerACertificate, selfSignedServerBCertificate, selfSignedServerCCertificate) =
                            CertificateUtils.RenewSelfSignedNodesCertificates(basePath, $"RavenDB-24742-{suffix}", $"RavenDB-24742-{suffix}-ca", with2Eku: with2Eku);

                        basePath = Path.Combine(basePath, "renewed");

                        Directory.CreateDirectory(basePath);

                        break;
                    }
                default:
                    throw new InvalidOperationException("TODO arek");
            }

            // server certs
            using (var serverAPfx = File.Create(Path.Combine(basePath, "a.cluster.server.certificate.24742-cluster-different-server-cert.pfx")))
            {
                serverAPfx.Write(selfSignedServerACertificate);
                serverAPfx.Flush();
            }

            using (var serverBPfx = File.Create(Path.Combine(basePath, "b.cluster.server.certificate.24742-cluster-different-server-cert.pfx")))
            {
                serverBPfx.Write(selfSignedServerBCertificate);
                serverBPfx.Flush();
            }

            using (var serverCPfx = File.Create(Path.Combine(basePath, "c.cluster.server.certificate.24742-cluster-different-server-cert.pfx")))
            {
                serverCPfx.Write(selfSignedServerCCertificate);
                serverCPfx.Flush();
            }

            if (mode == Mode.LoadCaAndGenerateNewPk || mode == Mode.GenerateNew)
            {
                // generate client cert

                X509Certificate2 serverCert = CertificateLoaderUtil.CreateCertificate(selfSignedServerACertificate, flags: CertificateLoaderUtil.FlagsForExport);

                var rsaPrivateKey = serverCert.GetRSAPrivateKey();
                AsymmetricCipherKeyPair kp = DotNetUtilities.GetRsaKeyPair(rsaPrivateKey);

                var selfSignedClientCertificate = CertificateUtils.CreateSelfSignedClientCertificate($"RavenDB-24742-{suffix}", serverCert, kp.Private,
                    out var clientCertBytes, DateTime.Today.AddMonths(3));

                // pfx
                using (var clientPfx = File.Create(Path.Combine(basePath, "admin.client.certificate.24742-cluster-different-server-cert.pfx")))
                {
                    clientPfx.Write(clientCertBytes);
                    clientPfx.Flush();
                }

                // crt
                using (var clientCrt = File.Create(Path.Combine(basePath, "admin.client.certificate.24742-cluster-different-server-cert.crt")))
                {
                    var crtBytes = selfSignedClientCertificate.Export(X509ContentType.Cert);

                    clientCrt.Write(crtBytes);
                    clientCrt.Flush();
                }

                // key
                {
                    var keyBytes = rsaPrivateKey.ExportPkcs8PrivateKeyPem();

                    File.WriteAllText(Path.Combine(basePath, "admin.client.certificate.24742-cluster-different-server-cert.key"), keyBytes);
                }

                // CA
                if (caCertBytes != null)
                    File.WriteAllBytes(Path.Combine(basePath, "ca.pfx"), caCertBytes);
            }

        }
    }
}
