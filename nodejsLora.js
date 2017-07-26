

////////////////////////////////////////////////////
///////////////////// REQUIRE //////////////////////
////////////////////////////////////////////////////

var mqtt = require('mqtt'); //https://www.npmjs.com/package/mqtt
var mysql = require('mysql');

////////////////////////////////////////////////////
///////////////////// MQTT /////////////////////////
////////////////////////////////////////////////////
/** Subscription for a fifo (persisted) **/
var Topic = 'fifo/default'
const Broker_URL = 'mqtt://liveobjects.orange-business.com';
const Database_URL = 'localhost';

var options = {
	clientId: 'MyMQTT',
	port: 1883,
	username: 'payload',
	password: '********************************', //put your API KEY right here
	qos: 1,	
	keepalive : 30,
	clean: false,
};

var client  = mqtt.connect(Broker_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);

/** connect to mqtt **/
function mqtt_connect() {
  	console.log("MQTT::Connected");

    client.subscribe(Topic, mqtt_subscribe);
    console.log("MQTT::Subscribed to topic:", Topic);
};

/** subscribe to a topic **/
function mqtt_subscribe(err, granted) {
    console.log("Subscribed to " + Topic);
    if (err) {console.log(err);}
};

/** try to reconnect **/
function mqtt_reconnect(err) {
    console.log("Reconnect MQTT");
    if (err) {console.log(err);}
    client  = mqtt.close();
	client  = mqtt.connect(Broker_URL, options);
};

/** signal an error **/
function mqtt_error(err) {
    console.log("Error!");
	if (err) {console.log(err);}
};

/** close mqtt connection **/
function mqtt_close() {
	console.log("Close MQTT");
};

////////////////////////////////////////////////////
///////////////////// MYSQL ////////////////////////
////////////////////////////////////////////////////

//Create Connection
var connection = mysql.createConnection({
	host: Database_URL,
	user: "root",
	password: "SQLadmin",
	database: "mydb"
});

connection.connect(function(err) {
	if (err) throw err;
	console.log("Database Connected!");
});

//insert a row into the tbl_messages table
function insert_message(topic, payload, packet) {
	try { var devEUI = payload["metadata"]["network"]["lora"]["devEUI"]; 			} catch(err) {devEUI = 0;}
	try { var message = payload["value"]["payload"]; 								} catch(err) {message = 0;}
	try { var snr = payload["metadata"]["network"]["lora"]["snr"];					} catch(err) {snr = 0;}
	try { var devicePort = payload["metadata"]["network"]["lora"]["port"];			} catch(err) {devicePort = 0;}
	try { var rssi = payload["metadata"]["network"]["lora"]["rssi"];				} catch(err) {rssi = 0;}
	try { var fcnt = payload["metadata"]["network"]["lora"]["fcnt"];				} catch(err) {fcnt = 0;}
	try { var sql = "INSERT INTO ?? (??,??,??,??,??,??,??) VALUES (?,?,?,?,?,?,?)"; } catch(err) {sql = 0;}
	//INSERT INTO tbl_LoraBroker (deviceID, payload, topic, port, rssi, Enable) Values(x, x, x, x, x, x);
	if(devEUI == 0)
	{
		console.log("Invalid Payload");
	}
	else
	{
		var params = ['tbl_LoRaBroker', 'deviceID', 'payload', 'topic', 'port', 'rssi','snr','fcnt', devEUI, message, Topic, devicePort, rssi, snr, fcnt];
		sql = mysql.format(sql, params);	
		connection.query(sql, function (error, results) {
		if (error) throw error;
		try	{console.log("Message added by: " + devEUI);} catch (err){console.log(err);}
		}); 
	}
};	

//split a string into an array of substrings
function extract_string(message_str) {
	var message_arr = message_str.split(","); //convert to array	
	return message_arr;
};	

//count number of delimiters in a string
var delimiter = ",";
function countInstances(message_str) {
	var substrings = message_str.split(delimiter);
	//console.log(substrings);
	//console.log(substrings.length);
	return substrings.length - 1;
};


//receive a message from MQTT broker
function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString(); //convert byte array to string
	//console.log(message_str);
	//message_str = message_str.replace(/\n/, ''); //remove new line
	message_str = message_str.replace(/(?:\r\n|\r|\n)/g, ''); //remove new line
	//console.log(message_str);
	//payload syntax: clientID,topic,message
	if (countInstances(message_str) == 0 ) {
		console.log("Invalid payload");
		} else {	
		var payload = JSON.parse(message_str);
		insert_message(topic, payload, packet);
		//console.log(message_arr);
	}
	

};
