using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Numerics;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT
{

    public record NodeResource(string Host, int Port, string? Id)
    {
        public static NodeResource FromNode(Node node) =>
            new NodeResource(node.Host, node.Port, NodeId.ToString(node.Id));

        public Node ToNode() => new Node(Host, Port);
    }

    public record FindSuccessorResource(int Hops, NodeResource Successor);

    public sealed class NodeClient : IDisposable
    {
        private readonly HttpClient _httpClient;
        private JsonSerializerOptions _jsonOptions;

        public NodeClient(Node node)
        {
            var (host, port) = node;
            var handler = new HttpClientHandler();
#if DEBUG
            if (host == "localhost" || host == Dns.GetHostName())
            {
                handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
            }
#endif
            _httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri($"https://{host}:{port}/dht/v1/"),
            };

            _httpClient.DefaultRequestHeaders.Accept.Clear();
            _httpClient.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json")
            );
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        private async Task<T?> DeserializeAsync<T>(Stream stream)
        {
            return await JsonSerializer.DeserializeAsync<T>(stream, _jsonOptions);
        }

        public async Task<Node> GetSuccessorAsync()
        {
            var stream = await _httpClient.GetStreamAsync("successor");
            var result = await DeserializeAsync<Node>(stream);
            return result ?? await Task.FromException<Node>(new InvalidOperationException());
        }

        public async Task<Node?> GetPredecessorAsync()
        {
            try
            {
                var stream = await _httpClient.GetStreamAsync("predecessor");
                return await DeserializeAsync<Node>(stream);
            }
            catch(HttpRequestException)
            {
                return null;
            }
        }

        public async Task<FindSuccessorResponse> FindSuccessorAsync(BigInteger nodeId, int hops = 0)
        {
            var nodeIdString = NodeId.ToString(nodeId);
            var stream = await _httpClient.GetStreamAsync($"successor/{nodeIdString}?hops={hops}");
            var result = await DeserializeAsync<FindSuccessorResource>(stream);

            if (result == null)
            {
                return await Task.FromException<FindSuccessorResponse>(new InvalidOperationException());
            }

            return new FindSuccessorResponse(result.Hops, result.Successor.ToNode());
        }

        public async Task<Node[]> GetSuccessorsAsync()
        {
            var stream = await _httpClient.GetStreamAsync("successors");
            var result = await DeserializeAsync<Node[]>(stream);
            return result ?? await Task.FromException<Node[]>(new InvalidOperationException());
        }

        public async Task NotifyAsync(Node node)
        {
            await _httpClient.PostAsync($"notify?Host={node.Host}&Port={node.Port}", new StringContent(""));
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}