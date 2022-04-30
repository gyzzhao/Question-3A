using consumer.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace consumer
{
    public class RabbitMQWorker: BackgroundService
    {
        private readonly ILogger<RabbitMQWorker> _logger;
        private ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IModel _channel;


        public RabbitMQWorker(ILogger<RabbitMQWorker> logger)
        {
            _logger = logger;
        }


        public override Task StartAsync(CancellationToken cancellationToken)
        {

            _connectionFactory = new ConnectionFactory()
            {
                //HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST"),
                //Port = Convert.ToInt32(Environment.GetEnvironmentVariable("RABBITMQ_PORT"))
                HostName = "localhost",
                Port = 31672
            
            };


            _connection = _connectionFactory.CreateConnection();

            _logger.LogInformation("RabbitMQ connection is opened.");

            _channel = _connection.CreateModel();

            _channel.ExchangeDeclare(exchange: "TaskQueue",
                        type: ExchangeType.Direct,
                        durable: true,
                        autoDelete: false);


            _channel.QueueDeclare(queue: "TaskQueue",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            _channel.QueueBind(queue: "TaskQueue",
                              exchange: "TaskQueue",
                              routingKey: "TaskQueue");

            return base.StartAsync(cancellationToken);
        }

        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                APIresult msg = JsonSerializer.Deserialize<APIresult>(message);
                _logger.LogInformation("Message consumed: " + msg.hasErr + "-" + msg.email);
                
            };

            consumer.Shutdown += OnConsumerShutdown;
            consumer.Registered += OnConsumerRegistered;
            consumer.Unregistered += OnConsumerUnregistered;
            consumer.ConsumerCancelled += OnConsumerConsumerCancelled;

            _channel.BasicConsume(queue: "TaskQueue", autoAck: true, consumer: consumer);
            await Task.CompletedTask;
        }


        private void OnConsumerConsumerCancelled(object sender, ConsumerEventArgs e) { }
        private void OnConsumerUnregistered(object sender, ConsumerEventArgs e) { }
        private void OnConsumerRegistered(object sender, ConsumerEventArgs e) { }
        private void OnConsumerShutdown(object sender, ShutdownEventArgs e) { }


        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await base.StopAsync(cancellationToken);
            _connection.Close();
            _logger.LogInformation("RabbitMQ connection is closed.");
        }


    }
}
