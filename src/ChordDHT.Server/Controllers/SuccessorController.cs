using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Serilog;

namespace ChordDHT.Server.Controllers
{
    [Route("dht/v1/successor")]
    public class SuccessorController : Controller
    {
        private readonly ChordServer _server;

        public SuccessorController(ChordServer server)
        {
            _server = server;
        }

        [HttpGet]
        public IActionResult Get() => Json(NodeResource.FromNode(_server.Successor));


        [HttpGet("{id}")]
        public async Task<IActionResult> Get(string id, [FromQuery] int hops)
        {
            var nodeId = NodeId.FromString(id);

            if (hops >= 3)
            {
                Log.Warning("Find successor at {Hops} hops from {LocalNode}", hops, _server.LocalNode);
            }
            else
            {
                Log.Information("Find successor at {Hops} hops from {LocalNode}", hops, _server.LocalNode);
            }

            var (hopsResult, successor) = await _server.FindSuccessorAsync(nodeId, hops);

            return Json(new FindSuccessorResource(hopsResult, NodeResource.FromNode(successor)));
        }
    }
}