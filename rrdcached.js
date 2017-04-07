'use strict';
//var RRDUtil = require('./util');
var util = require('util');
var net = require('net');

if(typeof String.prototype.startsWith === 'undefined'){
	String.prototype.startsWith = function(prefix) {
		return this.indexOf(prefix) === 0;
	};
}

/*
var escapeRegExp = function(str) {
    return str.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
};
*/

var RRDCache = function(){
	var client = null;
	var buffer = '';
	var lastReply = null;
	var running = false;
};

RRDCache.connect = function(address, callback){
	var self = this;
	self.pendingWrites = Array();
	if(typeof address === 'undefined'){
		address = 'unix:/tmp/rrdcached.sock';
	} else if (address === ''){
		address = ':42217';
	}
	var options = (address.startsWith('unix:/') ? {path: address.substring(5)} : {host: address.substring(0,address.indexOf(':')), port: address.substring(address.indexOf(':') + 1)});
	self.client = net.createConnection(options);
	self.client.on('error', function(error) {
		callback(error);
	});
	self.client.on('connect', function() {
		this.setEncoding('ascii');
		callback(null);
	});

	var status = null;
	self.client.on('data', function (data){
		if(!self.buffer){
			self.buffer = "";
		}
		self.buffer += data;
		if(status === null){
			status = parseInt(self.buffer.substring(0, self.buffer.indexOf(' ')));
		}
		var receivedAll = status < 0 || ((self.buffer.match(/\n/g) || []).length === status + 1);


		if(receivedAll){
			self.buffer = "";
			status = null;
			if (self.isBatch) {
				self.currentCallback(null, processData(data));
				return;
			}
			self.currentCallback(null, processData(data));
			if(self.pendingWrites.length){
				var pending = self.pendingWrites.pop();
				doWrite(self, pending.command, pending.callback);
			} else {
				self.running = false;
			}
		}
	});
};

RRDCache.write = function(command, callback){
	var self = this;
	if(!self.running){
		doWrite(self, command, callback);
	} else {
		self.pendingWrites.push({command: command, callback: callback});
	}
};

var doWrite = function(self, command, callback){
	self.running = true;
		var status = null;
		if(self.client !== null && command !== null){
			self.isBatch = command === 'BATCH' ? true : false;
			self.currentCallback = callback;
			command = command.trim();
			// append newline to terminate command
			if(command.substring(command.length - 1) != "\n"){
				command += "\n";
			}
			if(command === 'QUIT\n'){
				self.client.end(command, 'ascii');
				callback(null);
				self.pendingWrites = Array();
			} else {
				self.client.write(command, 'ascii');
			}
		} else {
			callback(new Error('Not Connected!'));
		}
};

var processData = function(data){
	var info = Array();
	var lines = data.split('\n');
	var statusIdx = lines[0].indexOf(' ');
	var status = parseInt(lines[0].substring(0, statusIdx));
	var statusLine = lines[0].substring(statusIdx + 1);
	var error = null;
	if (statusLine.substring(0,5) === 'error') {
		error = status > 0 ? true : false;
		if(error === true){
			for(var j = 1; j <= status; j++){
				info.push(lines[j]);
			}
			status = -status;
		}
	} else {
		error = status < 0 ? true : false;
		if(error === false){
			for(var i = 1; i <= status; i++){
				info.push(lines[i]);
			}
		}
	}
	RRDCache.lastReply = {
		statuscode: status,
		status: statusLine,
		error: error,
		info: info
	};
	return RRDCache.lastReply;
};

/*
var replaceN = function(value){
	return value.replace(/N/g, parseInt(Math.floor(Date.now()/1000)));
};
*/

RRDCache.flush = function(filename, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	RRDCache.write(util.format("FLUSH %s", filename), callback);
};

RRDCache.flushall = function(callback){
	RRDCache.write("FLUSHALL", callback);
};

RRDCache.help = function(command, callback){
	RRDCache.write(util.format("HELP %s", command), callback);
};

RRDCache.pending = function(filename, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	RRDCache.write(util.format("PENDING %s", filename), callback);
};

RRDCache.forget = function(filename, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	RRDCache.write(util.format("FORGET %s", filename), callback);
};

RRDCache.queue = function(callback){
	RRDCache.write("QUEUE", function(err, reply){
		if(err){
			callback(err);
			return;
		}
		reply.queue = {};
		for(var line of reply.info){
			var split = line.split(" ");
			reply.queue[split[1]] = parseInt(split[0]);
		}
		callback(null, reply);
	});
};

RRDCache.stats = function(callback){
	RRDCache.write("STATS", function(err, reply){
		if(err){
			callback(err);
			return;
		}
		reply.stats = {};
		for(var line of reply.info){
			var split = line.split(": ");
			reply.stats[split[0]] = parseInt(split[1]);
		}
		callback(null, reply);
	});
};

RRDCache.ping = function(callback){
	RRDCache.write("PING", callback);
};

RRDCache.update = function(filename, values, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	var newValues = "";
	if(Array.isArray(values)){
		for(var v of values){
			if(Array.isArray(v)){
				newValues += v.join(":") + " ";
			} else {
				newValues += v + " ";
			}
		}
	} else {
		newValues = values;
	}
	RRDCache.write(util.format("UPDATE %s %s", filename, newValues), callback);
};

RRDCache.fetch = function(filename, consFunction, start, end, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	if(consFunction === undefined){
		callback(new Error("No consolidation function specified!"));
		return;
	}
	if(consFunction != "AVERAGE" && consFunction != "MIN" && consFunction != "MAX" && consFunction != "LAST"){
		callback(new Error("Invalid consolidation function. Choose one of AVERAGE, MIN, MAX, LAST."));
		return;
	}
	RRDCache.write(util.format("FETCH %s %s %d %d", filename, consFunction, start, end), function(err, reply){
		if(err){
			callback(err);
			return;
		}
		reply.fetch = {};
		var dsName = null;
		var data = Array();
		for(var line of reply.info){
			var split = line.split(": ");
			switch(split[0]) {
				case 'Start':
				case 'End' :
					reply.fetch[split[0]] = split[1];
					continue;
				case 'DSName':
					dsName = split[1].split(" ");
					continue;
			}

			if (!/\D/.test(split[0])) {
				var values = split[1].split(" ");
				var tmp = {};
				if (dsName !== null && values.length === dsName.length) {
					for(var i = 0; i < values.length; i++){
						if (values[i] === '-nan') {
							tmp[dsName[i]] = NaN;
						} else {
							tmp[dsName[i]] = Number(values[i]);
						}
					}
					data.push({
						time: split[0],
						values: tmp
					});
				}
			}
		}
		reply.fetch.data = data;
		callback(null, reply);
	});
};

RRDCache.latest = function(filename, consFunction, callback){
	RRDCache.write(util.format("FETCH %s %s %d %d", filename, consFunction, -120, -60), function(err, reply){
		if(err){
			callback(err);
			return;
		}
		reply.latest = {};

		var data = Array();
		for (var i = 0; i < 3; i++) {
			var line = reply.info.pop();
			var split = line.split(": ");
			if (/\D/.test(split[0]) && split[0] !== 'DSName') {
				callback(new Error('Parse error'));
				return;
			}
			data.push(split[1].split(' '));
		}

		/*
		data[0] : latest
		data[1] : 1 minute ago
		data[2] : DS names
		 */

		if (data[0].length > 0 && data[0].length === data[1].length &&
			data[0].length === data[2].length) {
			var index = null;
			if (!isNaN(data[0][0]))	 {
				index = 0;
			} else if (!isNaN(data[1][0])) {
				index = 1;
			}

			if (index !== null) {
				for (var j = 0; j < data[2].length; j++) {
					reply.latest[data[2][j]] = Number(data[index][j]);
				}
			} else {
				for (var k = 0; k < data[2].length; k++) {
					reply.latest[data[2][k]] = NaN;
				}
			}
			callback(null, reply);
			return;
		}
		callback(new Error('Parse error'));
	});
};

RRDCache.first = function(){
	if(arguments.length >= 2 && arguments.length <=3){
		var filename = arguments[0];
		var rranum = arguments.length == 3 ? arguments[1] : 0;
		var callback = arguments.length == 3 ? arguments[2]: arguments[1];
		RRDCache.write(util.format("FIRST %s %d", filename, rranum), callback);
	} else {
		throw new Error('Invalid number of arguments!');
	}
};

RRDCache.last = function(filename, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	RRDCache.write(util.format("LAST %s", filename), callback);
};

RRDCache.info = function(filename, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	RRDCache.write(util.format("INFO %s", filename), callback);
};

RRDCache.create = function(filename, options, DSDefinitions, RRADefinitions, callback){
	if(filename === undefined){
		callback(new Error("No filename specified!"));
		return;
	}
	var stepsize = options !== null && options.stepsize !== undefined ? util.format("-s %d", options.stepsize) : "";
	var begintime = options !== null && options.begintime !== undefined ? util.format("-b %d", options.begintime) : "";
	var o = options !== null && options.o !== undefined && options.o ? "-O" : "";
	RRDCache.write(util.format("CREATE %s %s %s %s %s %s", filename, stepsize, begintime, o, DSDefinitions, RRADefinitions), callback);
};

RRDCache.batch = function(commands, callback){
	if(Array.isArray(commands)){
		var self = this;
		RRDCache.write('BATCH', function(err, reply) {
			if (reply.statuscode === 0) {
				var command = "";
				for(var c of commands){
					command += c + "\n";
				}
				command += ".";
				doWrite(self, command, callback);
				return;
			}
			if(self.pendingWrites.length){
				var pending = self.pendingWrites.pop();
				doWrite(self, pending.command, pending.callback);
			} else {
				self.running = false;
			}
			callback(new Error('Batch command error'));
		});
		return;
	}
	callback(new Error('commands is not an array'));
};

RRDCache.quit = function(callback){
	RRDCache.write("QUIT", callback);
};

RRDCache.getLastReply = function(){
	return this.lastReply;
};

//RRDCache.util = RRDUtil;
module.exports = RRDCache;
