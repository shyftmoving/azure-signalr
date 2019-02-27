// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.SignalR.Protocol;
using nl = NLog;

namespace Microsoft.Azure.SignalR.AspNet
{
    internal class ServiceConnectionManager : IServiceConnectionManager
    {
        private IReadOnlyDictionary<string, IServiceConnectionContainer> _serviceConnections = null;

        private readonly object _lock = new object();

        private readonly IReadOnlyList<string> _hubs;
        private readonly string _appName;

        private IServiceConnectionContainer _appConnection;

        private readonly nl.Logger _ourLogger = nl.LogManager.GetCurrentClassLogger(typeof(ServiceConnectionManager));

        public ServiceConnectionStatus Status => throw new NotSupportedException();

        public ServiceConnectionManager(string appName, IReadOnlyList<string> hubs)
        {
            _hubs = hubs ?? Array.Empty<string>();
            if (_hubs.Contains(appName))
            {
                throw new ArgumentException("App name should not be the same as hub name.");
            }

            _hubs = hubs;
            _appName = appName;
        }

        public void Initialize(Func<string, IServiceConnectionContainer> connectionGenerator)
        {
            try
            {
                if (connectionGenerator == null)
                {
                    throw new ArgumentNullException(nameof(connectionGenerator));
                }

                if (_serviceConnections != null)
                {
                    // TODO: log something to indicate the connection is already initialized.
                    return;
                }

                lock (_lock)
                {
                    if (_serviceConnections != null)
                    {
                        return;
                    }

                    var connections = new Dictionary<string, IServiceConnectionContainer>();

                    _appConnection = connectionGenerator(_appName);

                    foreach (var hub in _hubs)
                    {
                        var connection = connectionGenerator(hub);
                        connections.Add(hub, connection);
                    }

                    _serviceConnections = connections;
                }
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        public Task StartAsync()
        {
            try
            {
                return Task.WhenAll(GetConnections().Select(s => s.StartAsync()));
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        public IServiceConnectionContainer WithHub(string hubName)
        {
            try
            {
                if (_serviceConnections == null || !_serviceConnections.TryGetValue(hubName, out var connection))
                {
                    throw new KeyNotFoundException($"Service connection with Hub {hubName} does not exist");
                }

                return connection;
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        public virtual Task WriteAsync(ServiceMessage serviceMessage)
        {
            try
            {
                if (_appConnection == null)
                {
                    throw new InvalidOperationException("App connection is not yet initialized.");
                }

                return _appConnection.WriteAsync(serviceMessage);
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        public virtual Task WriteAsync(string partitionKey, ServiceMessage serviceMessage)
        {
            try
            {
                if (_appConnection == null)
                {
                    throw new InvalidOperationException("App connection is not yet initialized.");
                }

                return _appConnection.WriteAsync(partitionKey, serviceMessage);
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        private IEnumerable<IServiceConnectionContainer> GetConnections()
        {
            if (_appConnection != null)
            {
                yield return _appConnection;
            }

            if (_serviceConnections != null)
            {
                foreach (var conn in _serviceConnections)
                {
                    yield return conn.Value;
                }
            }
        }
    }
}
