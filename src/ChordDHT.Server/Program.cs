using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace ChordDHT.Server
{
    public static class Program
    {

        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .CreateLogger();

            try
            {
                var randomPort = args.Contains("--random-port");

                var builder = WebApplication.CreateBuilder(args.Where(arg => arg != "--random-port").ToArray());
                var port = GetPort(randomPort, builder) ?? 5000;
                var url = $"https://*:{port}";

                builder.WebHost.UseUrls(url);

                // Add services to the container.
                builder.Services.AddControllers();
                builder.Services.AddSwaggerGen(
                    c => { c.SwaggerDoc("v1", new() { Title = "ChordDHT.Server", Version = "v1" }); });

                var node = new Node(Dns.GetHostName(), port);
                var server = new ChordServer(node);
                server.Start();

                builder.Services.AddSingleton(server);

                WebApplication app = builder.Build();

                // Configure the HTTP request pipeline.
                if (builder.Environment.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                    app.UseSwagger();
                    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "ChordDHT.Server v1"));
                }

                app.UseHttpsRedirection();
                app.UseAuthorization();
                app.MapControllers();

                var seedHost = app.Configuration.GetValue<string?>("seed-host");
                var seedPort = app.Configuration.GetValue<int?>("seed-port");

                if (seedHost != null && seedPort != null)
                {
                    await server.JoinAsync(new Node(seedHost, seedPort.Value));
                }


                var addresses = app.Urls;
                if (addresses is null)
                {
                    throw new InvalidOperationException($"Changing the URL is not supported because no valid {nameof(IServerAddressesFeature)} was found.");
                }
                if (addresses.IsReadOnly)
                {
                    throw new InvalidOperationException($"Changing the URL is not supported because {nameof(IServerAddressesFeature.Addresses)} {nameof(ICollection<string>.IsReadOnly)}.");
                }

                addresses.Clear();
                addresses.Add(url);

                await app.StartAsync();


                while (true)
                {
                    switch (char.ToUpperInvariant(Console.ReadKey(true).KeyChar))
                    {
                        case 'I':
                            PrintNodeInfo(server, false);
                            continue;

                        case 'X':
                            PrintNodeInfo(server, true);
                            continue;

                        case 'Q':
                            break;

                        case 'R':
                            await PrintRing(server.LocalNode);
                            continue;

                        case '?':
                        default:
                            Console.WriteLine("Get Server [I]nfo, E[x]tended Info, [Q]uit, or Get Help[?]");
                            continue;
                    }
                    break;
                }

                await server.DepartAsync();
                await app.StopAsync();
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Application terminated unexpectedly");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        private static async Task PrintRing(Node node)
        {
            List<Node> nodes = new() { node };
            var current = node;
            while (true)
            {
                using var client = new NodeClient(current);
                current = await client.GetSuccessorAsync();
                if (node == current)
                {
                    PrintHeader("RING:");
                    foreach (var node1 in nodes)
                    {
                        Console.WriteLine(node1);
                    }
                    return;
                }

                nodes.Add(current);
            }

        }

        private static void PrintNodeInfo(ChordServer server, bool extended)
        {
            var successor = server.Successor;
            var predecessor = server.Predecessor;
            var fingerTable = server.FingerTable;
            var successorCache = server.Successors;
            var localNode = server.LocalNode;

            var successorString = successor.ToString();
            var predecessorString = predecessor != null ? predecessor.ToString() : "NULL";

            PrintHeader("NODE INFORMATION:");
            Console.WriteLine($"Successor: {successorString}");
            Console.WriteLine($"Local Node: {localNode}");
            Console.WriteLine($"Predecessor: {predecessorString}");
            Console.WriteLine();

            if (extended)
            {
                PrintHeader("SUCCESSORS:");
                for (var i = 0; i < successorCache.Length; i++)
                {
                    if (successorCache[i] != localNode)
                    {
                        Console.WriteLine($"{i}: {successorCache[i]}");
                    }
                }

                Console.WriteLine();

                PrintHeader("FINGER TABLE:");
                foreach (var entry in fingerTable)
                {
                    Console.WriteLine($"0x{entry.StartValue:x8}: {entry.Successor}");
                }
                Console.WriteLine();
            }
        }

        static void PrintHeader(string s)
        {
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine(s);
            Console.ResetColor();
        }

        private const int DefaultPort = 5000;

        private static int? GetPort(bool randomPort, WebApplicationBuilder builder)
        {
            if (randomPort)
            {
                var rnd = new Random();
                return rnd.Next(1000, 20000);
            }

            var port = builder.Configuration.GetValue<int?>("port");
            if (port != null)
                return port;

            // https://localhost:5001;http://localhost:5000
            var urlstring = builder.Configuration.GetValue<string?>("ASPNETCORE_URLS");
            if (urlstring == null) return null;

            var urls = urlstring.Split(";");

            foreach (var url in urls)
            {
                if (Uri.TryCreate(url, UriKind.Absolute, out var result) && result.Scheme == "https")
                {
                    return result.Port;
                }

            }

            return null;
        }
    }
}