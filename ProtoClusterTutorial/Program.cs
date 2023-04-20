using ProtoClusterTutorial;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddActorSystem();

builder.Services.AddHostedService<ActorSystemClusterHostedService>();

builder.Services.AddHostedService<SmartBulbSimulator>();

var app = builder.Build();

var loggerFactory = app.Services.GetRequiredService<ILoggerFactory>();
Proto.Log.SetLoggerFactory(loggerFactory);

app.MapGet("/", () => Task.FromResult("Hello, Proto.Cluster!"));

app.Run();