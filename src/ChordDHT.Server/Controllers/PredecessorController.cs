using Microsoft.AspNetCore.Mvc;

namespace ChordDHT.Server.Controllers
{
    [Route("dht/v1/predecessor")]
    public class PredecessorController : Controller
    {
        private readonly ChordServer _server;

        public PredecessorController(ChordServer server)
        {
            _server = server;
        }

        [HttpGet]
        public IActionResult Get()
        {
            var predecessor = _server.Predecessor;
            if (predecessor == null)
                return NotFound();

            return Json(NodeResource.FromNode(predecessor));
        }
    }
}