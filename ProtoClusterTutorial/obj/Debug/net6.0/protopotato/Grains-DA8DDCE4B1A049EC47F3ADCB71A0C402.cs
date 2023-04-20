
#nullable enable
#pragma warning disable 1591
using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Proto;
using Proto.Cluster;
using Microsoft.Extensions.DependencyInjection;

namespace ProtoClusterTutorial
{
    public static partial class GrainExtensions
    {
        public static SmartBulbGrainClient GetSmartBulbGrain(this global::Proto.Cluster.Cluster cluster, string identity) => new SmartBulbGrainClient(cluster, identity);

        public static SmartBulbGrainClient GetSmartBulbGrain(this global::Proto.IContext context, string identity) => new SmartBulbGrainClient(context.System.Cluster(), identity);
    }

    public abstract class SmartBulbGrainBase
    {
        protected global::Proto.IContext Context { get; }
        protected global::Proto.ActorSystem System => Context.System;
        protected global::Proto.Cluster.Cluster Cluster => Context.System.Cluster();
    
        protected SmartBulbGrainBase(global::Proto.IContext context)
        {
            Context = context;
        }
        
        public virtual Task OnStarted() => Task.CompletedTask;
        public virtual Task OnStopping() => Task.CompletedTask;
        public virtual Task OnStopped() => Task.CompletedTask;
        public virtual Task OnReceive() => Task.CompletedTask;

        public virtual async Task TurnOn(Action respond, Action<string> onError)
        {
            try
            {
                await TurnOn();
                respond();
            }
            catch (Exception x)
            {
                onError(x.ToString());
            }
        }
        public virtual async Task TurnOff(Action respond, Action<string> onError)
        {
            try
            {
                await TurnOff();
                respond();
            }
            catch (Exception x)
            {
                onError(x.ToString());
            }
        }
    
        public abstract Task TurnOn();
        public abstract Task TurnOff();
    }

    public class SmartBulbGrainClient
    {
        private readonly string _id;
        private readonly global::Proto.Cluster.Cluster _cluster;

        public SmartBulbGrainClient(global::Proto.Cluster.Cluster cluster, string id)
        {
            _id = id;
            _cluster = cluster;
        }

        public async Task<Google.Protobuf.WellKnownTypes.Empty?> TurnOn(CancellationToken ct)
        {
            var gr = new global::Proto.Cluster.GrainRequestMessage(0, null);
            //request the RPC method to be invoked
            var res = await _cluster.RequestAsync<object>(_id, SmartBulbGrainActor.Kind, gr, ct);

            return res switch
            {
                // normal response
                Google.Protobuf.WellKnownTypes.Empty message => global::Proto.Nothing.Instance,
                // enveloped response
                global::Proto.Cluster.GrainResponseMessage grainResponse => global::Proto.Nothing.Instance,
                // error response
                global::Proto.Cluster.GrainErrorResponse grainErrorResponse => throw new Exception(grainErrorResponse.Err),
                // timeout (when enabled by ClusterConfig.LegacyRequestTimeoutBehavior), othwerwise TimeoutException is thrown
                null => null,
                // unsupported response
                _ => throw new NotSupportedException($"Unknown response type {res.GetType().FullName}")
            };
        }
        
        public async Task<Google.Protobuf.WellKnownTypes.Empty?> TurnOn(global::Proto.ISenderContext context, CancellationToken ct)
        {
            var gr = new global::Proto.Cluster.GrainRequestMessage(0, null);
            //request the RPC method to be invoked
            var res = await _cluster.RequestAsync<object>(_id, SmartBulbGrainActor.Kind, gr, context, ct);

            return res switch
            {
                // normal response
                Google.Protobuf.WellKnownTypes.Empty message => global::Proto.Nothing.Instance,
                // enveloped response
                global::Proto.Cluster.GrainResponseMessage grainResponse => global::Proto.Nothing.Instance,
                // error response
                global::Proto.Cluster.GrainErrorResponse grainErrorResponse => throw new Exception(grainErrorResponse.Err),
                // timeout (when enabled by ClusterConfig.LegacyRequestTimeoutBehavior), othwerwise TimeoutException is thrown
                null => null,
                // unsupported response
                _ => throw new NotSupportedException($"Unknown response type {res.GetType().FullName}")
            };
        }
        public async Task<Google.Protobuf.WellKnownTypes.Empty?> TurnOff(CancellationToken ct)
        {
            var gr = new global::Proto.Cluster.GrainRequestMessage(1, null);
            //request the RPC method to be invoked
            var res = await _cluster.RequestAsync<object>(_id, SmartBulbGrainActor.Kind, gr, ct);

            return res switch
            {
                // normal response
                Google.Protobuf.WellKnownTypes.Empty message => global::Proto.Nothing.Instance,
                // enveloped response
                global::Proto.Cluster.GrainResponseMessage grainResponse => global::Proto.Nothing.Instance,
                // error response
                global::Proto.Cluster.GrainErrorResponse grainErrorResponse => throw new Exception(grainErrorResponse.Err),
                // timeout (when enabled by ClusterConfig.LegacyRequestTimeoutBehavior), othwerwise TimeoutException is thrown
                null => null,
                // unsupported response
                _ => throw new NotSupportedException($"Unknown response type {res.GetType().FullName}")
            };
        }
        
        public async Task<Google.Protobuf.WellKnownTypes.Empty?> TurnOff(global::Proto.ISenderContext context, CancellationToken ct)
        {
            var gr = new global::Proto.Cluster.GrainRequestMessage(1, null);
            //request the RPC method to be invoked
            var res = await _cluster.RequestAsync<object>(_id, SmartBulbGrainActor.Kind, gr, context, ct);

            return res switch
            {
                // normal response
                Google.Protobuf.WellKnownTypes.Empty message => global::Proto.Nothing.Instance,
                // enveloped response
                global::Proto.Cluster.GrainResponseMessage grainResponse => global::Proto.Nothing.Instance,
                // error response
                global::Proto.Cluster.GrainErrorResponse grainErrorResponse => throw new Exception(grainErrorResponse.Err),
                // timeout (when enabled by ClusterConfig.LegacyRequestTimeoutBehavior), othwerwise TimeoutException is thrown
                null => null,
                // unsupported response
                _ => throw new NotSupportedException($"Unknown response type {res.GetType().FullName}")
            };
        }
    }

    public class SmartBulbGrainActor : global::Proto.IActor
    {
        public const string Kind = "SmartBulbGrain";

        private SmartBulbGrainBase? _inner;
        private global::Proto.IContext? _context;
        private readonly Func<global::Proto.IContext, global::Proto.Cluster.ClusterIdentity, SmartBulbGrainBase> _innerFactory;
    
        public SmartBulbGrainActor(Func<global::Proto.IContext, global::Proto.Cluster.ClusterIdentity, SmartBulbGrainBase> innerFactory)
        {
            _innerFactory = innerFactory;
        }

        public async Task ReceiveAsync(global::Proto.IContext context)
        {
            switch (context.Message)
            {
                case Started msg: 
                {
                    _context = context;
                    var id = context.Get<global::Proto.Cluster.ClusterIdentity>()!; // Always populated on startup
                    _inner = _innerFactory(context, id);
                    await _inner.OnStarted();
                    break;
                }
                case Stopping _:
                {
                    await _inner!.OnStopping();
                    break;
                }
                case Stopped _:
                {
                    await _inner!.OnStopped();
                    break;
                }    
                case GrainRequestMessage(var methodIndex, var r):
                {
                    switch (methodIndex)
                    {
                        case 0:
                        {   
                            await _inner!.TurnOn(Respond, OnError);

                            break;
                        }
                        case 1:
                        {   
                            await _inner!.TurnOff(Respond, OnError);

                            break;
                        }
                        default:
                            OnError($"Invalid client contract. Unexpected Index {methodIndex}");
                            break;
                    }

                    break;
                }
                default:
                {
                    await _inner!.OnReceive();
                    break;
                }
            }
        }

        private void Respond<T>(T response) where T : global::Google.Protobuf.IMessage => _context!.Respond(response is not null ? response : new global::Proto.Cluster.GrainResponseMessage(response));
        private void Respond() => _context!.Respond(new global::Proto.Cluster.GrainResponseMessage(null));
        private void OnError(string error) => _context!.Respond(new global::Proto.Cluster.GrainErrorResponse { Err = error });

        public static global::Proto.Cluster.ClusterKind GetClusterKind(Func<global::Proto.IContext, global::Proto.Cluster.ClusterIdentity, SmartBulbGrainBase> innerFactory)
            => new global::Proto.Cluster.ClusterKind(Kind, global::Proto.Props.FromProducer(() => new SmartBulbGrainActor(innerFactory)));

        public static global::Proto.Cluster.ClusterKind GetClusterKind<T>(global::System.IServiceProvider serviceProvider) where T : SmartBulbGrainBase
            => new global::Proto.Cluster.ClusterKind(Kind, global::Proto.Props.FromProducer(() => new SmartBulbGrainActor((ctx, id) => global::Microsoft.Extensions.DependencyInjection.ActivatorUtilities.CreateInstance<T>(serviceProvider, ctx, id))));
    }
}

