const EventEmitter = require("events");
const mqtt = require("mqtt");
const os = require("os");

let callId = 0;

module.exports = broker => {

	let gwId = `${os.hostname}_${process.pid}`;
	let pendingCalls = {};

	let client = mqtt.connect("mqtt://" + broker);

	client.on("connect", () => {
		client.subscribe("+/rpc");
		client.subscribe("+/event/+/+/+");
	});

	client.on("message", (topic, message) => {

		topic = topic.split("/");

		if (topic.length === 2 && topic[0] === gwId && topic[1] === "rpc") {
			message = JSON.parse(message.toString());
			let call = pendingCalls[message.tag];
			if (call) {
				call(message);
			}
		}

	});

	return {

		attach(deviceId, serviceName, service = {}) {

			service.device = deviceId;
			service.name = serviceName;
			service.events = new EventEmitter();

			let proxy = new Proxy(service, {
				get(target, property, receiver) {
					
					if (property === "inspect" || property.toString() === "Symbol(util.inspect.custom)") {
						return null;
					}
					
					return target[property] || ((args) => {

						return new Promise((resolve, reject) => {

							let id = callId++;

							console.debug("Calling", `${deviceId}.${serviceName}.${property}(${JSON.stringify(args)})`);

							let timeout = setTimeout(() => {
								delete pendingCalls[id];
								reject(`Call to ${deviceId}.${serviceName}.${property} timed out`);
							}, 3000);

							pendingCalls[id] = (reply) => {
								clearTimeout(timeout);
								delete pendingCalls[id];
								console.debug("Call reply", reply);
								if (reply.error) {
									reject(reply.error.message || reply.error);
								} else {
									resolve(reply.result);
								}
							};

							client.publish(`${deviceId}/rpc`, JSON.stringify({
								src: gwId,
								tag: id,
								method: `${serviceName}.${property}`,
								args
							}));

						});

					});
				}
			});

			client.on("message", async (topic, message) => {

				topic = topic.split("/");

				if (
						topic.length === 5 &&
						topic[0] === deviceId &&
						topic[1] === "event" &&
						topic[2] === serviceName
						) {

					let event = topic[4];
					message = JSON.parse(message.toString());
					try {
						await service.events.emit(event, message);
					} catch(e) {
						console.error("Error in service event handler:", e);
					}
				}
			});

			return proxy;
		}
	};

};