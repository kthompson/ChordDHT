using Microsoft.AspNetCore.Mvc;

namespace ChordDHT.Server.Controllers
{
    [Route("dht/v1/notify")]
    public class NotifyController : Controller
    {
        private readonly ChordServer _server;

        public NotifyController(ChordServer server)
        {
            _server = server;
        }

        [HttpPost]
        public IActionResult Post([FromQuery] NodeResource node)
        {
            _server.Notify(new Node(node.Host, node.Port));

            return NoContent();
        }
    }
}