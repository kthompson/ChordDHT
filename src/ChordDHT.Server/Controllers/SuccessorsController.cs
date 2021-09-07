using System.Linq;
using Microsoft.AspNetCore.Mvc;

namespace ChordDHT.Server.Controllers
{
    [Route("dht/v1/successors")]
    public class SuccessorsController : Controller
    {
        private readonly ChordServer _server;

        public SuccessorsController(ChordServer server)
        {
            _server = server;
        }

        [HttpGet]
        public IActionResult Index() => Json(_server.Successors.Select(NodeResource.FromNode));
    }
}