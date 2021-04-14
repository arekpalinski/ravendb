using System;
using System.IO;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ProxyWebSocketConnection : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ProxyWebSocketConnection>(nameof(ProxyWebSocketConnection));

        private readonly CancellationToken _token;
        private readonly Uri _webSocketUri;
        private readonly ClientWebSocket _remoteWebSocket;
        private readonly WebSocket _localWebSocket;
        private readonly IMemoryContextPool _contextPool;
        private Task _localToRemote;
        private Task _remoteToLocal;

        public ProxyWebSocketConnection(WebSocket localWebSocket, string nodeUrl, string websocketEndpoint, IMemoryContextPool contextPool, CancellationToken token)
        {
            if (string.IsNullOrEmpty(nodeUrl))
                throw new ArgumentException("Node url cannot be null or empty", nameof(nodeUrl));

            if (string.IsNullOrEmpty(websocketEndpoint))
                throw new ArgumentException("Endpoint cannot be null or empty", nameof(websocketEndpoint));

            if (websocketEndpoint.StartsWith("/") == false)
                throw new ArgumentException("Endpoint must starts with '/' character", nameof(websocketEndpoint));

            _localWebSocket = localWebSocket;
            _contextPool = contextPool;
            _token = token;
            _webSocketUri = new Uri($"{nodeUrl.Replace("http", "ws", StringComparison.OrdinalIgnoreCase)}{websocketEndpoint}");
            _remoteWebSocket = new ClientWebSocket();
        }

        public Task Establish(X509Certificate2 certificate)
        {
            if (certificate != null) 
                _remoteWebSocket.Options.ClientCertificates.Add(certificate);
            
            return _remoteWebSocket.ConnectAsync(_webSocketUri, _token);
        }

        public async Task RelayData()
        {
            _localToRemote = ForwardLocalToRemote();
            _remoteToLocal = ForwardRemoteToLocal();

            await Task.WhenAny(_localToRemote, _remoteToLocal);

            await _localToRemote;
        }

        private async Task ForwardLocalToRemote()
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment))
            {
                try
                {
                    while (true) // TODO arek
                    {
                        var buffer = segment.Memory.Memory;

                        var receiveResult = await _localWebSocket.ReceiveAsync(buffer, _token);

                        await _remoteWebSocket.SendAsync(buffer.Slice(0, receiveResult.Count), WebSocketMessageType.Text, false, _token);
                    }
                }
                catch (IOException ex)
                {
                    /* Client was disconnected, write to log */
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Client was disconnected", ex);
                }
                catch (Exception ex)
                {
                    // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                    if (ex is WebSocketException webSocketException
                        && webSocketException.WebSocketErrorCode == WebSocketError.InvalidState
                        && _localWebSocket.State == WebSocketState.CloseReceived)
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private async Task ForwardRemoteToLocal()
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
            {
                try
                {
                    var segments = new[] { segment1, segment2 };
                    int index = 0;
                    var receiveAsync = _remoteWebSocket.ReceiveAsync(segments[index].Memory.Memory, _token);

                    var result = await receiveAsync;


                    var bufferToForward = segments[index];
                    index++;

                    receiveAsync = _remoteWebSocket.ReceiveAsync(segments[index].Memory.Memory, _token);

                    while (true)
                    {
                        await _localWebSocket.SendAsync(bufferToForward.Memory.Memory.Slice(0, result.Count), result.MessageType, result.EndOfMessage, _token);

                        result = await receiveAsync;

                        bufferToForward = segments[index];

                        if (++index >= segments.Length)
                            index = 0;

                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

            //using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            //using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment))
            //{
            //    try
            //    {
            //        while (_localToRemote.IsCompleted == false && (_remoteWebSocket.State == WebSocketState.Open || _remoteWebSocket.State == WebSocketState.CloseSent))
            //        {
            //            var buffer = segment.Memory.Memory;

            //            var receiveResult = await _remoteWebSocket.ReceiveAsync(buffer, _token).ConfigureAwait(false);

            //            await _localWebSocket.SendAsync(buffer.Slice(0, receiveResult.Count), WebSocketMessageType.Text, false, _token);
            //        }
            //    }
            //    catch (Exception e)
            //    {
            //        Console.WriteLine(e);
            //        throw;
            //    }
            //}
        }

        public void Dispose()
        {
            _remoteWebSocket.Dispose();
        }
    }
}
