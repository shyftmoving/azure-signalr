﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.Azure.SignalR.Protocol;
using nl = NLog;

namespace Microsoft.Azure.SignalR.AspNet
{
    internal class ServiceMessageBus : MessageBus
    {
        private readonly IMessageParser _parser;
        private readonly IServiceConnectionManager _serviceConnectionManager;
        private readonly IClientConnectionManager _clientConnectionManager;
        private readonly IAckHandler _ackHandler;
        private readonly nl.Logger _ourLogger = nl.LogManager.GetCurrentClassLogger(typeof(ServiceMessageBus));

        public ServiceMessageBus(IDependencyResolver resolver) : base(resolver)
        {
            // TODO: find a more decent way instead of DI, it can be easily overriden
            _serviceConnectionManager = resolver.Resolve<IServiceConnectionManager>() ?? throw new ArgumentNullException(nameof(IServiceConnectionManager));
            _clientConnectionManager = resolver.Resolve<IClientConnectionManager>() ?? throw new ArgumentNullException(nameof(IClientConnectionManager));
            _parser = resolver.Resolve<IMessageParser>() ?? throw new ArgumentNullException(nameof(IMessageParser));
            _ackHandler = resolver.Resolve<IAckHandler>() ?? throw new ArgumentNullException(nameof(IAckHandler));
        }

        public override Task Publish(Message message)
        {
            try
            {
                var messages = _parser.GetMessages(message).ToList();
                if (messages.Count == 0)
                {
                    return Task.CompletedTask;
                }

                if (messages.Count == 1)
                {
                    return ProcessMessage(messages[0]);
                }

                return Task.WhenAll(messages.Select(m => ProcessMessage(m)));
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        private Task ProcessMessage(AppMessage message)
        {
            try
            {
                if (message is HubMessage hubMessage)
                {
                    return WriteMessage(_serviceConnectionManager.WithHub(hubMessage.HubName), message);
                }

                return WriteMessage(_serviceConnectionManager, message);
            }
            catch (Exception e)
            {
                _ourLogger.Error(e);
                throw e;
            }
        }

        private async Task WriteMessage(IServiceConnectionContainer connection, AppMessage appMessage)
        {
            try
            {
                var message = appMessage.Message;
                switch (message)
                {
                    // For group related messages, make sure messages are written to the same partition
                    case JoinGroupMessage joinGroupMessage:
                        try
                        {
                            await connection.WriteAsync(joinGroupMessage.GroupName, joinGroupMessage);
                        }
                        finally
                        {
                            _ackHandler.TriggerAck(appMessage.RawMessage.CommandId);
                        }
                        break;
                    case LeaveGroupMessage leaveGroupMessage:
                        try
                        {
                            await connection.WriteAsync(leaveGroupMessage.GroupName, leaveGroupMessage);
                        }
                        finally
                        {
                            _ackHandler.TriggerAck(appMessage.RawMessage.CommandId);
                        }
                        break;
                    case GroupBroadcastDataMessage groupBroadcastMessage:
                        await connection.WriteAsync(groupBroadcastMessage.GroupName, groupBroadcastMessage);
                        break;
                    case ConnectionDataMessage connectionDataMessage:
                        if (_clientConnectionManager.TryGetServiceConnection(connectionDataMessage.ConnectionId, out var serviceConnection))
                        {
                            // If the client connection is connected to local server connection, send back directly from the established server connection
                            await serviceConnection.WriteAsync(message);
                        }
                        else
                        {
                            await connection.WriteAsync(message);
                        }
                        break;
                    default:
                        await connection.WriteAsync(message);
                        break;
                }
            }
            catch (Exception e) {
                _ourLogger.Error(e);
                throw e;
            }
        }
    }
}
