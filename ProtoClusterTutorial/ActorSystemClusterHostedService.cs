using Proto;
using Proto.Cluster;

namespace ProtoClusterTutorial;

public class ActorSystemClusterHostedService : IHostedService
{
    private readonly ActorSystem _actorSystem;

    public async Task TurnTheLightOnInTheKitchen(CancellationToken ct)
    {
        SmartBulbGrainClient smartBulbGrainClient = _actorSystem
            .Cluster()
            .GetSmartBulbGrain(identity: "kitchen");

        await smartBulbGrainClient.TurnOn(ct);
    }

    public ActorSystemClusterHostedService(ActorSystem actorSystem)
    {
        _actorSystem = actorSystem;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting a cluster member");

        await _actorSystem
            .Cluster()
            .StartMemberAsync();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Shutting down a cluster member");

        await _actorSystem
            .Cluster()
            .ShutdownAsync();
    }
}