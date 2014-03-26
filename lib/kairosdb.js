/*
 * Flush stats to KairosDB (https://code.google.com/p/kairosdb/)
 *
 * This backend sends a metric to KairosDB for every metric received via the
 * statsd protocol. There is no aggregation.
 *
 * To enable this backend, include 'statsd-kairosdb-backend' in the backends
 * configuration array:
 *
 *   backends: ['statsd-kairosdb-backend']
 *
 * The backend will read the configuration options from the following
 * 'kairosdb' hash defined in the main statsd config file:
 *
 * kairosdb: {
 *   host: 'localhost',      // KairosDB host. (default localhost)
 *   port: 4242,             // KairosDB port. (default 4242)
 *   reconnectInterval: 1000 // KairosDB reconnect interval. (default 1000)
 * }
 *
 */
var net = require('net');

function KairosdbBackend(startupTime, config, events, logger) {
  var self = this;

  self.defaultHost = '127.0.0.1';
  self.defaultPort = 4242;
  self.defaultReconnectInterval = 1000;
  self.host = self.defaultHost;
  self.port = self.defaultPort;
  self.reconnectInterval = self.defaultReconnectInterval;
  self.debug = config.debug;
  self.logger = logger;

  // set up namespaces
  self.globalNamespace  = [];
  self.counterNamespace = [];
  self.timerNamespace   = [];
  self.gaugesNamespace  = [];
  self.setsNamespace    = [];
  self.tags = { source: "statsd" };

  var globalPrefix;
  var prefixCounter;
  var prefixTimer;
  var prefixGauge;
  var prefixSet;

  if (config.kairosdb) {
    self.host = config.kairosdb.host || self.defaultHost;
    self.port = config.kairosdb.port || self.defaultPort;
    self.reconnectInterval = config.kairosdb.reconnectInterval || self.defaultReconnectInterval;
    globalPrefix = config.kairosdb.globalPrefix || "stats";
    prefixCounter = config.kairosdb.prefixCounter || "counters";
    prefixTimer = config.kairosdb.prefixTimer || "timers";
    prefixGauge = config.kairosdb.prefixGauge || "gauges";
    prefixSet = config.kairosdb.prefixSet || "sets";
    // In order to unconditionally add this string, it either needs to be
    // a single space if it was unset, OR surrounded by a . and a space if
    // it was set.
    self.globalSuffix = config.kairosdb.globalSuffix ? '.' + config.kairosdb.globalSuffix : '';
    if (config.kairosdb.tags !== undefined) {
      for (var prop in config.kairosdb.tags) {
        self.tags[prop] = config.kairosdb.tags[prop];
      }
    }
  }

  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";

  if (globalPrefix !== "") {
    self.globalNamespace.push(globalPrefix);
    self.counterNamespace.push(globalPrefix);
    self.timerNamespace.push(globalPrefix);
    self.gaugesNamespace.push(globalPrefix);
    self.setsNamespace.push(globalPrefix);
  }

  if (prefixCounter !== "") {
    self.counterNamespace.push(prefixCounter);
  }
  if (prefixTimer !== "") {
    self.timerNamespace.push(prefixTimer);
  }
  if (prefixGauge !== "") {
    self.gaugesNamespace.push(prefixGauge);
  }
  if (prefixSet !== "") {
    self.setsNamespace.push(prefixSet);
  }

  self.kairosdbStats = {};
  self.kairosdbStats.last_flush = startupTime;
  self.kairosdbStats.last_exception = startupTime;
  self.kairosdbStats.flush_time = 0;
  self.kairosdbStats.flush_length = 0;

  self.flushInterval = config.flushInterval;

  self.flush_counts = typeof(config.flush_counts) === "undefined" ? true : config.flush_counts;


  self.kairosdbConnected = false;
  self.notifiedNoConnection = false;

  self.kairosdb = self.dbConnect();

  // events.on('packet', function (packet, rinfo) {
  //   try {
  //     if (self.kairosdbConnected) {
  //       self.process(packet, rinfo);
  //     } else {
  //       if (!self.notifiedNoConnection) {
  //         self.notifiedNoConnection = true;
  //         self.logger.log('Not connected yet.');
  //       }
  //     }
  //   } catch (e) {
  //     self.logger.log(e);
  //   }
  // });

  events.on('flush', function(ts, metrics) {
    self.flush_stats(ts, metrics);
  });
  events.on('status', function(writeCb) {
    self.backend_status(writeCb);
  });

}

KairosdbBackend.prototype.dbConnect= function () {
  var self = this,
      host = self.host,
      port = self.port,
      conn = net.createConnection({host: host, port: port});

  conn.on('error', function (e) {
    self.logger.log("KairosDB: host:" + host + ", port:" + port + " - " + e);
    self.kairosdbConnected = false;

    setTimeout(function () {
      self.kairosdb = self.dbConnect();
    }, self.reconnectInterval);
  });

  conn.on('connect', function () {
    self.kairosdbConnected = true;
    self.notifiedNoConnection = false;

    self.logger.log('Connected to ' + [host, port].join(':'));
  });

  return conn;
}

KairosdbBackend.prototype.send = function (ts, key, value, client) {
  var self = this,
      statString = ['put', key, ts, value].join(' ');

  var tags = "";
  for(var prop in self.tags) {
    if (self.tags != null) {
      tags += " " + prop + "=" + self.tags[prop];
    }
  }
  if (client) {
    tags += " client=" + client;
  }
  statString += tags;

  if (self.debug) {
    self.logger.log(statString);
  }

  self.kairosdb.write(statString + "\n");
}

KairosdbBackend.prototype.process = function (packet, rinfo) {
  var self = this,
      ts = Math.round(new Date().getTime() / 1000),
      client = rinfo.address;

  /* From stats.js. */
  var packet_data = packet.toString();

  if (packet_data.indexOf("\n") > -1) {
    var metrics = packet_data.split("\n");
  } else {
    var metrics = [ packet_data ] ;
  }

  for (var midx in metrics) {
    if (metrics[midx].length === 0) {
      continue;
    }
    var bits = metrics[midx].toString().split(':');
    var key = bits.shift()
      .replace(/\s+/g, '_')
      .replace(/\//g, '-')
      .replace(/[^a-zA-Z_\-0-9\.]/g, '');

    if (bits.length === 0) {
      bits.push("1");
    }

    for (var i = 0; i < bits.length; i++) {
      var fields = bits[i].split("|");
      if (fields[1] === undefined) {
        self.logger.log('Bad line: ' + fields + ' in msg "' + metrics[midx] +'"');
        continue;
      }
      var metric_type = fields[1].trim();
      if (metric_type === "ms") {
        self.send(ts, key, Number(fields[0] || 0), client);
      } else if (metric_type === "g") {
        if (fields[0].match(/^[-+]/)) {
          self.logger.log('Sending gauges with +/- is not supported yet.');
        } else {
          self.send(ts, key, Number(fields[0] || 0), client);
        }
      } else if (metric_type === "s") {
        self.logger.log('Sets not supported yet.');
      } else {
        self.send(ts, key, Number(fields[0] || 1), client);
      }
    }
  }
}

KairosdbBackend.prototype.post_stats = function(statString) {
  var self = this;
  var last_flush = self.kairosdbStats.last_flush || 0;
  var last_exception = self.kairosdbStats.last_exception || 0;
  var flush_time = self.kairosdbStats.flush_time || 0;
  var flush_length = self.kairosdbStats.flush_length || 0;
  var ts = Math.round(new Date().getTime() / 1000);
  var ts_suffix = ' ' + ts + "\n";
  var namespace = self.globalNamespace.concat(prefixStats).join(".");
  self.send(ts, namespace + '.kairosdbStats.last_exception' + self.globalSuffix, last_exception);
  self.send(ts, namespace + '.kairosdbStats.last_flush'     + self.globalSuffix, last_flush);
  self.send(ts, namespace + '.kairosdbStats.flush_time'     + self.globalSuffix, flush_time);
  self.send(ts, namespace + '.kairosdbStats.flush_length'   + self.globalSuffix, flush_length);

  var starttime = Date.now();
  self.kairosdbStats.flush_time = (Date.now() - starttime);
//  self.kairosdbStats.flush_length = statString.length;
  self.kairosdbStats.last_flush = Math.round(new Date().getTime() / 1000);
//    self.kairosdbStats.last_exception = Math.round(new Date().getTime() / 1000);
};

KairosdbBackend.prototype.flush_stats = function(ts, metrics) {
  var self = this;
  var starttime = Date.now();
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    var namespace = self.counterNamespace.concat(key);
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

    self.send(ts, namespace.concat('rate').join(".")  + self.globalSuffix, valuePerSecond);
    if (self.flush_counts) {
      self.send(ts, namespace.concat('count').join(".") + self.globalSuffix, value);
    }

    numStats += 1;
  }

  for (key in timer_data) {
    var namespace = self.timerNamespace.concat(key);
    var the_key = namespace.join(".");
    for (timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        self.send(ts, the_key + '.' + timer_data_key + self.globalSuffix, timer_data[key][timer_data_key]);
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (self.debug) {
            self.logger.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          self.send(ts, the_key + '.' + timer_data_key + '.' + timer_data_sub_key + self.globalSuffix,
                        timer_data[key][timer_data_key][timer_data_sub_key]);
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    var namespace = self.gaugesNamespace.concat(key);
    self.send(ts, namespace.join(".") + self.globalSuffix, gauges[key]);
    numStats += 1;
  }

  for (key in sets) {
    var namespace = self.setsNamespace.concat(key);
    self.send(ts, namespace.join(".") + '.count' + self.globalSuffix, sets[key].values().length);
    numStats += 1;
  }

  var namespace = self.globalNamespace.concat(prefixStats);
  self.send(ts, namespace.join(".") + '.numStats' + self.globalSuffix, numStats);
  self.send(ts, namespace.join(".") + '.kairosdbStats.calculationtime' + self.globalSuffix, (Date.now() - starttime));
  for (key in statsd_metrics) {
    var the_key = namespace.concat(key);
    self.send(ts, the_key.join(".") + self.globalSuffix, statsd_metrics[key]);
  }
  self.post_stats();

  if (self.debug) {
   self.logger.log("numStats: " + numStats);
  }
};

KairosdbBackend.prototype.backend_status = function(writeCb) {
  var self = this;
  for (var stat in self.kairosdbStats) {
    writeCb(null, 'kairosdb', stat, kairosdbStats[stat]);
  }
};

exports.init = function (startupTime, config, events, logger) {
  var kairosdb = new KairosdbBackend(startupTime, config, events, logger);

  return true;
}
