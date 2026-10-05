from flask import Flask, jsonify, request, send_from_directory, Response
import json
import os
import random
import time
import socket
import queue
import threading
from datetime import datetime

app = Flask(__name__)

# ==========================================
# FILE LOCATIONS (ABSOLUTE PATHS)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TRAINS_FILE = os.path.join(DATA_DIR, "trains.json")
HELP_FILE = os.path.join(DATA_DIR, "help_requests.json")

os.makedirs(DATA_DIR, exist_ok=True)

# ==========================================
# NETWORK IP DETECTION FOR HOTSPOT DEMOS
# ==========================================
def get_local_ip():
    """Detects active local Wi-Fi or Hotspot IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not send packets; retrieves interface used to reach public route
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

# ==========================================
# SSE EVENT BROADCAST SYSTEM (REAL-TIME PUSH)
# ==========================================
subscribers_lock = threading.Lock()
subscribers = []

def broadcast_event(event_type, data):
    """Pushes real-time updates to all connected browser tabs in <50ms."""
    with subscribers_lock:
        closed_queues = []
        for q in subscribers:
            try:
                q.put_nowait({"type": event_type, "data": data})
            except queue.Full:
                closed_queues.append(q)
        for q in closed_queues:
            subscribers.remove(q)

# ==========================================
# PREVENT BROWSER CACHING
# ==========================================
@app.after_request
def add_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "-1"
    return response

# ==========================================
# DATA ACCESS HELPERS
# ==========================================
def load_train_data():
    try:
        if os.path.exists(TRAINS_FILE):
            with open(TRAINS_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
    except Exception as e:
        print(f"[Error loading trains.json]: {e}")
    return {}

def save_train_data(data):
    try:
        with open(TRAINS_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        return True
    except Exception as e:
        print(f"[Error saving trains.json]: {e}")
        return False

def load_help_requests():
    try:
        if os.path.exists(HELP_FILE):
            with open(HELP_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
    except Exception as e:
        print(f"[Error loading help_requests.json]: {e}")
    return []

def save_help_requests(data):
    try:
        with open(HELP_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        return True
    except Exception as e:
        print(f"[Error saving help_requests.json]: {e}")
        return False

# ==========================================
# STATIC PAGE ROUTES
# ==========================================
@app.route("/")
def passenger_portal():
    return send_from_directory(BASE_DIR, "index.html")

@app.route("/staff")
def staff_dashboard():
    return send_from_directory(BASE_DIR, "staff.html")

# ==========================================
# NETWORK & SYSTEM TELEMETRY
# ==========================================
@app.route("/api/system/network", methods=["GET"])
def get_network_info():
    local_ip = get_local_ip()
    port = 5000
    return jsonify({
        "local_ip": local_ip,
        "port": port,
        "passenger_url": f"http://{local_ip}:{port}",
        "staff_url": f"http://{local_ip}:{port}/staff",
        "hostname": socket.gethostname(),
        "edge_node": "Station Edge Node #04 (Madurai Hub)"
    })

# ==========================================
# REAL-TIME SSE STREAM ROUTE
# ==========================================
@app.route("/api/stream")
def sse_stream():
    """Server-Sent Events endpoint for instant UI updates."""
    def event_stream():
        q = queue.Queue(maxsize=50)
        with subscribers_lock:
            subscribers.append(q)
        try:
            init_payload = {
                "trains": load_train_data(),
                "requests": load_help_requests(),
                "timestamp": datetime.now().strftime("%I:%M:%S %p")
            }
            yield f"event: init\ndata: {json.dumps(init_payload)}\n\n"

            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"event: {msg['type']}\ndata: {json.dumps(msg['data'])}\n\n"
                except queue.Empty:
                    yield f"event: ping\ndata: {json.dumps({'time': time.time()})}\n\n"
        except GeneratorExit:
            with subscribers_lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(event_stream(), mimetype="text/event-stream")

# ==========================================
# TRAIN API ROUTES
# ==========================================
@app.route("/api/trains", methods=["GET"])
def get_all_trains():
    return jsonify(load_train_data())

@app.route("/api/train/<train_no>", methods=["GET"])
def get_train(train_no):
    data = load_train_data()
    if train_no in data:
        return jsonify(data[train_no])
    return jsonify({"error": "Train not found"}), 404

@app.route("/api/train/<train_no>/update", methods=["POST"])
def update_train(train_no):
    data = load_train_data()
    if train_no not in data:
        return jsonify({"error": "Train not found"}), 404

    update = request.get_json() or {}
    old_platform = data[train_no].get("platform", 2)
    new_platform = int(update.get("platform", old_platform))

    has_platform_changed = (old_platform != new_platform)

    train = data[train_no]
    train["previous_platform"] = old_platform if has_platform_changed else train.get("previous_platform", old_platform)
    train["platform"] = new_platform
    train["platform_changed"] = has_platform_changed or update.get("platform_changed", False)
    
    train["current_location"] = update.get("current_location", train.get("current_location", "Villupuram"))
    train["next_station"] = update.get("next_station", train.get("next_station", "Madurai"))
    train["delay"] = int(update.get("delay", train.get("delay", 0)))
    train["speed"] = int(update.get("speed", train.get("speed", 68)))
    train["status"] = update.get("status", train.get("status", "Running"))
    train["crowd_level"] = update.get("crowd_level", train.get("crowd_level", "Moderate"))
    train["edge_mode"] = update.get("edge_mode", train.get("edge_mode", "Normal"))
    train["last_updated"] = datetime.now().isoformat()

    stations = ["Chennai Egmore", "Villupuram", "Tiruchirappalli", "Dindigul", "Madurai"]
    if train["current_location"] in stations:
        idx = stations.index(train["current_location"])
        train["progress"] = int((idx / (len(stations) - 1)) * 100)

    save_train_data(data)

    broadcast_payload = {
        "train_no": train_no,
        "train": train,
        "platform_changed": has_platform_changed,
        "old_platform": old_platform,
        "new_platform": new_platform,
        "timestamp": datetime.now().strftime("%I:%M:%S %p")
    }
    broadcast_event("train_update", broadcast_payload)

    return jsonify({
        "success": True,
        "message": "Train telemetry updated on Local Station Edge Server",
        "train": train,
        "platform_changed": has_platform_changed
    })

# ==========================================
# 1-CLICK OPERATIONAL SCENARIOS RUNNER
# ==========================================
@app.route("/api/scenarios/apply", methods=["POST"])
def apply_scenario():
    """Applies realistic operational railway scenarios with 1 click."""
    scenario_id = (request.get_json() or {}).get("scenario_id")
    data = load_train_data()
    train_no = "12641"
    train = data.get(train_no, {})

    if scenario_id == "pf2_signal_failure":
        train["previous_platform"] = train.get("platform", 2)
        train["platform"] = 4
        train["platform_changed"] = True
        train["status"] = "Platform Diverted (Signal Fault)"
        train["crowd_level"] = "High"
        train["delay"] = 12
        train["last_updated"] = datetime.now().isoformat()
        save_train_data(data)

        broadcast_event("train_update", {
            "train_no": train_no, "train": train,
            "platform_changed": True, "old_platform": 2, "new_platform": 4,
            "reason": "Signal fault detected on track circuit #02"
        })
        return jsonify({"success": True, "message": "Scenario applied: Platform 2 Track Circuit Fault ➔ Diverted to Platform 4"})

    elif scenario_id == "fob_stampede_risk":
        train["crowd_level"] = "Overcrowded"
        train["last_updated"] = datetime.now().isoformat()
        save_train_data(data)

        broadcast_event("train_update", {
            "train_no": train_no, "train": train,
            "crowd_alert": True, "crowd_level": "Overcrowded"
        })
        return jsonify({"success": True, "message": "Scenario applied: FOB-1 Stampede Bottleneck Triggered"})

    elif scenario_id == "divyangjan_transit":
        reqs = load_help_requests()
        req_id = f"RN-{random.randint(1000, 9999)}"
        new_req = {
            "request_id": req_id,
            "type": "Divyangjan Wheelchair / Buggy",
            "train": train_no,
            "coach": "A1",
            "seat": "32",
            "timestamp": datetime.now().strftime("%I:%M %p"),
            "status": "Pending",
            "assigned_unit": "Unassigned",
            "notes": "Elderly passenger requires battery cart transit across platforms."
        }
        reqs.insert(0, new_req)
        save_help_requests(reqs)
        broadcast_event("help_request_created", new_req)
        return jsonify({"success": True, "message": "Scenario applied: Divyangjan Wheelchair Assistance Requested"})

    elif scenario_id == "reset_normal":
        train["platform"] = 2
        train["previous_platform"] = 2
        train["platform_changed"] = False
        train["status"] = "Running"
        train["delay"] = 0
        train["speed"] = 68
        train["crowd_level"] = "Moderate"
        train["edge_mode"] = "Normal"
        train["last_updated"] = datetime.now().isoformat()
        save_train_data(data)

        # Clear active requests
        save_help_requests([])

        broadcast_event("train_update", {
            "train_no": train_no, "train": train,
            "platform_changed": False, "old_platform": 2, "new_platform": 2
        })
        broadcast_event("help_request_updated", {})
        return jsonify({"success": True, "message": "All station parameters reset to Normal Green Corridor."})

    return jsonify({"error": "Unknown scenario"}), 400

# ==========================================
# DYNAMIC COACH-AWARE EGRESS ROUTING
# ==========================================
@app.route("/api/egress/routes", methods=["GET"])
def get_egress_routes():
    """Calculates personalized, collision-free crowd routing based on passenger coach."""
    coach = request.args.get("coach", "A1").upper()
    train_data = load_train_data().get("12641", {})
    current_pf = train_data.get("platform", 2)
    target_pf = 4 if current_pf == 2 else 2

    if "S" in coach:  # Sleeper coaches (S1 - S5, Southern end of train)
        route_info = {
            "coach": coach,
            "assigned_route": "South Foot-Over-Bridge (FOB-1)",
            "safety_status": "🟢 Optimal Flow",
            "traffic": "Light Traffic (85 passengers)",
            "distance": "30 meters",
            "eta": "1 min",
            "guidance": f"Exit Coach {coach} and proceed toward the Engine. Climb South FOB-1 directly to Platform {target_pf}.",
            "avoid_point": "Central Ramp R-1 (Cross-flow congestion)",
            "path_type": "south_fob"
        }
    elif coach in ["WHEELCHAIR", "DIVYANGJAN"]:
        route_info = {
            "coach": "Wheelchair Accessible",
            "assigned_route": "Central Gradient Ramp (R-1)",
            "safety_status": "♿ Barrier-Free Path",
            "traffic": "Dedicated Accessible Corridor",
            "distance": "40 meters",
            "eta": "2.5 mins",
            "guidance": "Follow yellow high-contrast tactile path to Central Ramp R-1. Electric Cart #02 is on the way.",
            "avoid_point": "Escalators & Stairs",
            "path_type": "central_ramp"
        }
    else:  # AC Coaches (B1, A1, A2, Northern end of train)
        route_info = {
            "coach": coach,
            "assigned_route": "North Escalator (ESC-2)",
            "safety_status": "🟢 Clear Flow",
            "traffic": "Moderate (115 passengers)",
            "distance": "45 meters",
            "eta": "1.5 mins",
            "guidance": f"Exit Coach {coach} toward Northern platform end. Board North Escalator ESC-2 to crossover to Platform {target_pf}.",
            "avoid_point": "South Foot-Over-Bridge (Avoid: 380+ passenger surge)",
            "path_type": "north_escalator"
        }

    return jsonify({
        "status": "success",
        "current_platform": current_pf,
        "target_platform": target_pf,
        "route": route_info
    })

# ==========================================
# CCTV AI TELEMETRY & MESH TOPOLOGY
# ==========================================
@app.route("/api/cctv/telemetry", methods=["GET"])
def get_cctv_telemetry():
    train_data = load_train_data().get("12641", {})
    crowd_level = train_data.get("crowd_level", "Moderate")

    if crowd_level in ["High", "Overcrowded", "Critical"]:
        heads_detected = random.randint(390, 450)
        density = round(random.uniform(3.6, 4.3), 2)
        threat = "CRITICAL"
        color = "#EF4444"
        intervention_needed = True
    elif crowd_level == "Moderate":
        heads_detected = random.randint(190, 240)
        density = round(random.uniform(1.9, 2.4), 2)
        threat = "MODERATE"
        color = "#F59E0B"
        intervention_needed = False
    else:
        heads_detected = random.randint(70, 110)
        density = round(random.uniform(0.7, 1.1), 2)
        threat = "NORMAL"
        color = "#10B981"
        intervention_needed = False

    return jsonify({
        "camera_id": "CAM-04 (Platform 2 Mid-Concourse)",
        "resolution": "1920x1080 @ 30 FPS",
        "inference_engine": "Edge NPU (Local Acceleration)",
        "heads_detected": heads_detected,
        "density_per_m2": density,
        "stampede_threshold": 3.5,
        "threat_level": threat,
        "color": color,
        "intervention_recommended": intervention_needed
    })

@app.route("/api/mesh/status", methods=["GET"])
def get_mesh_status():
    train_data = load_train_data().get("12641", {})
    is_outage = (train_data.get("edge_mode") == "Simulated Internet Outage / Station Mesh")

    nodes = [
        {"id": "station_edge", "name": "Station Edge Hub", "type": "Server", "status": "ONLINE", "ip": "192.168.1.100", "latency": "0.4ms"},
        {"id": "cctv_npu", "name": "CCTV Vision NPU", "type": "Sensor", "status": "ONLINE", "ip": "192.168.1.104", "latency": "1.2ms"},
        {"id": "platform_led", "name": "PF 2/4 Displays", "type": "Display", "status": "ONLINE", "ip": "192.168.1.112", "latency": "2.1ms"},
        {"id": "coach_ap", "name": "Coach A1 Mesh AP", "type": "AccessPoint", "status": "ONLINE", "ip": "192.168.1.130", "latency": "3.5ms"},
        {"id": "cloud_gateway", "name": "Cloud WAN Uplink", "type": "Gateway", "status": "OFFLINE" if is_outage else "ONLINE", "ip": "10.0.0.1", "latency": "OFFLINE" if is_outage else "45.0ms"}
    ]

    return jsonify({
        "mesh_mode": "Station Edge Mesh Local",
        "cloud_internet_active": not is_outage,
        "active_mesh_peers": 4 if is_outage else 5,
        "packet_loss": "0.00%",
        "nodes": nodes
    })

# ==========================================
# AI COPILOT & ASSISTANCE
# ==========================================
@app.route("/api/ai/suggestions", methods=["GET"])
def get_ai_suggestions():
    data = load_train_data()
    train_no = "12641"
    train = data.get(train_no, {})
    help_requests = load_help_requests()
    suggestions = []

    current_platform = train.get("platform", 2)
    crowd_level = train.get("crowd_level", "Moderate")
    
    if crowd_level in ["High", "Overcrowded", "Critical"]:
        recommended_pf = 4 if current_platform == 2 else 2
        suggestions.append({
            "id": "sug_platform_congestion",
            "category": "Anti-Stampede AI Mitigation",
            "badge": "Critical Safety",
            "badge_color": "danger",
            "title": f"Divert Pandian Express: Platform {current_platform} ➔ Platform {recommended_pf}",
            "description": f"CAM-04 vision AI detected crowd density exceeding 3.5 p/m² on Platform {current_platform}. Autonomous routing to Platform {recommended_pf} will clear FOB bottlenecks.",
            "action_type": "update_platform",
            "action_data": { "platform": recommended_pf, "crowd_level": "Moderate" }
        })
    else:
        suggestions.append({
            "id": "sug_platform_efficiency",
            "category": "Platform Flow Optimization",
            "badge": "Optimal Corridor",
            "badge_color": "info",
            "title": f"Maintain Platform {current_platform} Green Corridor",
            "description": f"Platform {current_platform} crowd flow is {crowd_level.lower()}. Maintain current berth indicators.",
            "action_type": "none",
            "action_data": {}
        })

    active_emergencies = [r for r in help_requests if r.get("status") in ["Pending", "Acknowledged"]]
    if active_emergencies:
        top_req = active_emergencies[0]
        req_type = top_req.get("type", "Assistance")
        coach = top_req.get("coach", "A1")
        is_divyangjan = "Divyangjan" in req_type or "Wheelchair" in req_type or "Buggy" in req_type
        
        suggestions.append({
            "id": "sug_emergency_triage",
            "category": "Divyangjan & Care Protocol" if is_divyangjan else "Station Care Triage",
            "badge": "Priority Protocol" if is_divyangjan else "Emergency",
            "badge_color": "info" if is_divyangjan else "danger",
            "title": f"Deploy Station Buggy / Assistance for {req_type} (Coach {coach})",
            "description": f"Passenger at Coach {coach} requested priority {req_type}. Station master should alert Platform {current_platform} team.",
            "action_type": "dispatch_emergency",
            "action_data": {
                "request_id": top_req.get("request_id"),
                "status": "Dispatched",
                "assigned_unit": "Electric Buggy #02 (Divyangjan Transit)",
                "notes": f"Dispatched to Coach {coach} ramp area."
            }
        })

    return jsonify({
        "status": "success",
        "timestamp": datetime.now().strftime("%I:%M:%S %p"),
        "suggestions": suggestions
    })

@app.route("/api/help", methods=["GET"])
def get_help_requests():
    return jsonify(load_help_requests())

@app.route("/api/help", methods=["POST"])
def create_help_request():
    data = request.get_json() or {}
    requests = load_help_requests()

    req_id = f"RN-{random.randint(1000, 9999)}"
    req_type = data.get("type", "General Assistance")
    new_req = {
        "request_id": req_id,
        "type": req_type,
        "train": data.get("train", "12641"),
        "coach": data.get("coach", "A1"),
        "seat": str(data.get("seat", "32")),
        "timestamp": datetime.now().strftime("%I:%M %p"),
        "status": "Pending",
        "assigned_unit": "Unassigned",
        "notes": data.get("notes", "Awaiting station master dispatch")
    }

    requests.insert(0, new_req)
    save_help_requests(requests)
    broadcast_event("help_request_created", new_req)

    return jsonify({
        "success": True,
        "request_id": req_id,
        "request": new_req
    })

@app.route("/api/help/<request_id>/respond", methods=["POST"])
def respond_help_request(request_id):
    requests = load_help_requests()
    body = request.get_json() or {}
    
    updated_req = None
    for r in requests:
        if r.get("request_id") == request_id:
            r["status"] = body.get("status", "Dispatched")
            r["assigned_unit"] = body.get("assigned_unit", "Station Quick Response Team")
            r["notes"] = body.get("notes", "Team dispatched to coach location.")
            r["responded_at"] = datetime.now().strftime("%I:%M %p")
            updated_req = r
            break

    if not updated_req:
        return jsonify({"error": "Request ID not found"}), 404

    save_help_requests(requests)
    broadcast_event("help_request_updated", updated_req)

    return jsonify({
        "success": True,
        "message": f"Assistance request {request_id} updated.",
        "request": updated_req
    })

# ==========================================
# SERVER ENTRY POINT
# ==========================================
if __name__ == "__main__":
    local_ip = get_local_ip()
    print("")
    print("==================================================")
    print("       RAILNEXIS STATION EDGE SERVER ACTIVE       ")
    print("==================================================")
    print(f"Local Laptop:     http://127.0.0.1:5000")
    print(f"Phone / Hotspot:  http://{local_ip}:5000")
    print(f"Staff Dashboard:  http://127.0.0.1:5000/staff")
    print("==================================================")
    print("")

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True
    )