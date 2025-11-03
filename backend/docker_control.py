import yaml
import os
from docker import DockerClient
from docker.errors import NotFound

DOCKER_SOCK = os.getenv("DOCKER_SOCK", "unix:///var/run/docker.sock")
COMPOSE_FILE = os.getenv("DOCKER_COMPOSE_FILE", "./docker-compose.yml")


def get_client():
    """Lazily create a DockerClient. Creating at import time can fail if the
    docker socket isn't mounted yet (and will crash the whole app during import).
    This helper creates a client when needed so imports remain safe.
    """
    return DockerClient(base_url=DOCKER_SOCK)


def _parse_ports(ports_list):
    ports = {}
    for p in ports_list:
        # formats like "9092:9092" or "0.0.0.0:9092:9092"
        parts = p.split(":")
        if len(parts) == 2:
            host, container = parts
        elif len(parts) == 3:
            _, host, container = parts
        else:
            continue
        ports[f"{container}/tcp"] = int(host)
    return ports


def _parse_volumes(vols_list):
    vols = {}
    for v in vols_list:
        # host:container
        if ":" in v:
            host, cont = v.split(":", 1)
            vols[os.path.abspath(host)] = {"bind": cont, "mode": "rw"}
    return vols


def start_service_from_compose(service_name: str):
    with open(COMPOSE_FILE, "r") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    svc = services.get(service_name)
    if not svc:
        raise RuntimeError(f"Service {service_name} not found in compose file")

    image = svc.get("image")
    name = svc.get("container_name") or service_name
    hostname = svc.get("hostname")
    env = svc.get("environment", {})
    ports = _parse_ports(svc.get("ports", []))
    volumes = _parse_volumes(svc.get("volumes", []))
    restart_policy = {"Name": svc.get("restart", "unless-stopped")}

    # If container already exists, start it
    try:
        c = get_client().containers.get(name)
        if c.status != "running":
            c.start()
        return c
    except NotFound:
        pass

    # Create container
    networking = None
    try:
        net = get_client().networks.get("streaming-platform")
        networking = net.name
    except Exception:
        networking = None

    container = get_client().containers.run(
        image,
        name=name,
        hostname=hostname,
        environment=env,
        ports=ports if ports else None,
        volumes=volumes if volumes else None,
        detach=True,
        restart_policy=restart_policy,
        network=networking,
    )
    return container


def stop_service(service_name: str):
    # service_name may be container_name or compose service name
    try:
        c = get_client().containers.get(service_name)
        c.stop()
        return True
    except NotFound:
        # try to find by container_name in compose
        with open(COMPOSE_FILE, "r") as f:
            compose = yaml.safe_load(f)
        svc = compose.get("services", {}).get(service_name, {})
        name = svc.get("container_name")
        if not name:
            return False
        try:
            c = get_client().containers.get(name)
            c.stop()
            return True
        except NotFound:
            return False


def restart_service(service_name: str):
    try:
        c = get_client().containers.get(service_name)
        c.restart()
        return True
    except NotFound:
        # try container_name mapping
        with open(COMPOSE_FILE, "r") as f:
            compose = yaml.safe_load(f)
        svc = compose.get("services", {}).get(service_name, {})
        name = svc.get("container_name")
        if not name:
            return False
        try:
            c = get_client().containers.get(name)
            c.restart()
            return True
        except NotFound:
            return False


def remove_service(service_name: str):
    try:
        c = get_client().containers.get(service_name)
        c.remove(force=True)
        return True
    except NotFound:
        with open(COMPOSE_FILE, "r") as f:
            compose = yaml.safe_load(f)
        svc = compose.get("services", {}).get(service_name, {})
        name = svc.get("container_name")
        if not name:
            return False
        try:
            c = get_client().containers.get(name)
            c.remove(force=True)
            return True
        except NotFound:
            return False
