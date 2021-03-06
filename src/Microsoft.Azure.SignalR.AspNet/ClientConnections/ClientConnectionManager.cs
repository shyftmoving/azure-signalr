﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Hosting;
using Microsoft.AspNet.SignalR.Hubs;
using Microsoft.Azure.SignalR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Owin;
using nl = NLog;

namespace Microsoft.Azure.SignalR.AspNet
{
    internal class ClientConnectionManager : IClientConnectionManager
    {
        private readonly HubConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly nl.Logger _ourLogger = nl.LogManager.GetCurrentClassLogger(typeof (ClientConnectionManager));

        private readonly ConcurrentDictionary<string, IServiceConnection> _clientConnections = new ConcurrentDictionary<string, IServiceConnection>();

        public ClientConnectionManager(HubConfiguration configuration)
        {
            _configuration = configuration;
            var loggerFactory = configuration.Resolver.Resolve<ILoggerFactory>() ?? NullLoggerFactory.Instance;
            _logger = loggerFactory.CreateLogger<ClientConnectionManager>();
        }

        public IServiceTransport CreateConnection(OpenConnectionMessage message, IServiceConnection serviceConnection)
        {
            try
            {
                var dispatcher = new ClientConnectionHubDispatcher(_configuration, message.ConnectionId);
                dispatcher.Initialize(_configuration.Resolver);

                var responseStream = new MemoryStream();
                var hostContext = GetHostContext(message, responseStream, serviceConnection);

                if (dispatcher.Authorize(hostContext.Request))
                {
                    // ProcessRequest checks if the connectionToken matches "{connectionid}:{userName}" format with context.User
                    _ = dispatcher.ProcessRequest(hostContext);

                    // TODO: check for errors written to the response
                    if (hostContext.Response.StatusCode != 200)
                    {
                        Log.ProcessRequestError(_logger, message.ConnectionId, hostContext.Request.QueryString.ToString());
                        var errorResponse = GetContentAndDispose(responseStream);
                        throw new InvalidOperationException(errorResponse);
                    }

                    _clientConnections.TryAdd(message.ConnectionId, serviceConnection);
                    return (AzureTransport)hostContext.Environment[AspNetConstants.Context.AzureSignalRTransportKey];
                }

                // This happens when hub is not found
                Debug.Fail("Unauthorized");
                throw new InvalidOperationException("Unable to authorize request");
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        public bool TryGetServiceConnection(string key, out IServiceConnection serviceConnection)
        {
            try
            {
                return _clientConnections.TryGetValue(key, out serviceConnection);
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        internal HostContext GetHostContext(OpenConnectionMessage message, Stream responseStream, IServiceConnection serviceConnection)
        {
            var connectionId = message.ConnectionId;
            var context = new OwinContext();
            var response = context.Response;
            var request = context.Request;

            response.Body = responseStream;

            var user = request.User = message.GetUserPrincipal();

            request.Path = new PathString("/");

            string queryString = message.QueryString;
            if (queryString.Length > 0)
            {
                // The one from Azure SignalR always contains a leading '?' character however the Owin one does not
                if (queryString[0] == '?')
                {
                    queryString = queryString.Substring(1);
                }

                request.QueryString = new QueryString(queryString);
            }

            if (message.Headers != null)
            {
                foreach (var pair in message.Headers)
                {
                    request.Headers.Add(pair.Key, pair.Value);
                }
            }

            context.Environment[AspNetConstants.Context.AzureServiceConnectionKey] = serviceConnection;
            return new HostContext(context.Environment);
        }

        internal static string GetContentAndDispose(MemoryStream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        private sealed class ClientConnectionHubDispatcher : HubDispatcher
        {
            private readonly string _connectionId;

            public ClientConnectionHubDispatcher(HubConfiguration config, string connectionId) : base(config)
            {
                _connectionId = connectionId;
            }

            protected override bool TryGetConnectionId(HostContext context, string connectionToken, out string connectionId, out string message, out int statusCode)
            {
                connectionId = _connectionId;
                message = null;
                statusCode = 200;
                return true;
            }
        }

        private static class Log
        {
            private static readonly Action<ILogger, string, string, Exception> _processRequestError =
                LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(1, "ProcessRequestError"), "ProcessRequest for {connectionId} fails with {queryString} ");

            public static void ProcessRequestError(ILogger logger, string connectionId, string queryString)
            {
                _processRequestError(logger, connectionId, queryString, null);
            }
        }
    }
}
