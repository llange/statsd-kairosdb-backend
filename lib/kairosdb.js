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
 /* jshint quotmark: false */
"use strict";
var http = require('http');

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
  self.prefixStats		= config.prefixStats !== undefined ? config.prefixStats : "statsd";

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

  events.on('flush', function(ts, metrics) {
    self.flush_stats(ts, metrics);
  });
  events.on('status', function(writeCb) {
    self.backend_status(writeCb);
  });

}


KairosdbBackend.prototype.assembleDatapoints = function(name, datapoints, tags) {
  var payload = {
    name: name,
    datapoints: datapoints,
    tags: tags
  };

  return payload;
};

KairosdbBackend.prototype.send = function (ts_milliseconds, key, value, client) {
  var self = this;
  var tags = self.tags;

  if (client) {
    tags.client = client;
  }

  var point = self.assembleDatapoints(key, [[ts_milliseconds, value]], tags);

  if (self.debug) {
    self.logger.log(JSON.stringify(point), 'DEBUG');
  }

  return point;
};

KairosdbBackend.prototype.post_stats = function(statString) {
  var self = this;
  var last_flush = self.kairosdbStats.last_flush || 0;
  var last_exception = self.kairosdbStats.last_exception || 0;
  var flush_time = self.kairosdbStats.flush_time || 0;
  var flush_length = self.kairosdbStats.flush_length || 0;
  var ts_milliseconds = new Date().getTime();	// ts is in milliseconds !!!
  var namespace = self.globalNamespace.concat(self.prefixStats).join(".");
  var points = [];
  points.push(self.send(ts_milliseconds, namespace + '.kairosdbStats.last_exception' + self.globalSuffix, last_exception));
  points.push(self.send(ts_milliseconds, namespace + '.kairosdbStats.last_flush'     + self.globalSuffix, last_flush));
  points.push(self.send(ts_milliseconds, namespace + '.kairosdbStats.flush_time'     + self.globalSuffix, flush_time));
  points.push(self.send(ts_milliseconds, namespace + '.kairosdbStats.flush_length'   + self.globalSuffix, flush_length));

  var starttime = Date.now();
  self.kairosdbStats.flush_time = (Date.now() - starttime);
//  self.kairosdbStats.flush_length = statString.length;
  self.kairosdbStats.last_flush = Math.round(new Date().getTime() / 1000);
//    self.kairosdbStats.last_exception = Math.round(new Date().getTime() / 1000);
	return points;
};

KairosdbBackend.prototype.flush_stats = function(ts, metrics) {
	// ts is in seconds (coming from statsd)
  var ts_milliseconds = 1000 * ts;
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
  var namespace = null;
  var the_key = null;
  var points = [];

  for (key in counters) {
    namespace = self.counterNamespace.concat(key);
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

    points.push(self.send(ts_milliseconds, namespace.concat('rate').join(".")  + self.globalSuffix, valuePerSecond));
    if (self.flush_counts) {
      points.push(self.send(ts_milliseconds, namespace.concat('count').join(".") + self.globalSuffix, value));
    }

    numStats += 1;
  }

  for (key in timer_data) {
    namespace = self.timerNamespace.concat(key);
    the_key = namespace.join(".");
    for (timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        points.push(self.send(ts_milliseconds, the_key + '.' + timer_data_key + self.globalSuffix, timer_data[key][timer_data_key]));
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (self.debug) {
            self.logger.log(timer_data[key][timer_data_key][timer_data_sub_key].toString(), 'DEBUG');
          }
          points.push(self.send(ts_milliseconds, the_key + '.' + timer_data_key + '.' + timer_data_sub_key + self.globalSuffix,
                                  timer_data[key][timer_data_key][timer_data_sub_key]));
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    namespace = self.gaugesNamespace.concat(key);
    points.push(self.send(ts_milliseconds, namespace.join(".") + self.globalSuffix, gauges[key]));
    numStats += 1;
  }

  for (key in sets) {
    namespace = self.setsNamespace.concat(key);
    points.push(self.send(ts_milliseconds, namespace.join(".") + '.count' + self.globalSuffix, sets[key].values().length));
    numStats += 1;
  }

  namespace = self.globalNamespace.concat(self.prefixStats);
  points.push(self.send(ts_milliseconds, namespace.join(".") + '.numStats' + self.globalSuffix, numStats));
  points.push(self.send(ts_milliseconds, namespace.join(".") + '.kairosdbStats.calculationtime' + self.globalSuffix, (Date.now() - starttime)));
  for (key in statsd_metrics) {
    the_key = namespace.concat(key);
    points.push(self.send(ts_milliseconds, the_key.join(".") + self.globalSuffix, statsd_metrics[key]));
  }
  points.push.apply(points, self.post_stats());

  self.httpPOST(points);

  if (self.debug) {
   self.logger.log("numStats: " + numStats, 'DEBUG');
  }
};

KairosdbBackend.prototype.httpPOST = function (points) {
  /* Do not send if there are no points. */
  if (!points.length) { return; }

  var self = this;

  if (self.debug) {
    self.logger.log('Sending ' + points.length + ' different points via HTTP', 'DEBUG');
  }

  var options = {
    hostname: self.host,
    port: self.port,
    path: '/api/v1/datapoints',
    method: 'POST',
    agent: false // Is it okay to use "undefined" here? (keep-alive)
  };

  var req = http.request(options);

  req.on('response', function (res) {
    var status = res.statusCode;

    if (status !== 204) {	// Correct output/status is : "HTTP/1.1 204 No Content."
      self.logger.log('HTTP Error: ' + status, 'ERROR');
    }
  });

  req.on('error', function (e, i) {
    self.logger.log(e, 'ERROR');
  });

  if (self.debug) {
    var str = JSON.stringify(points),
        size = (Buffer.byteLength(str) / 1024).toFixed(2);

    self.logger.log('Payload size ' + size + ' KB', 'DEBUG');
  }

  req.write(JSON.stringify(points));
  req.end();
};

KairosdbBackend.prototype.backend_status = function(writeCb) {
  var self = this;
  for (var stat in self.kairosdbStats) {
    writeCb(null, 'kairosdb', stat, self.kairosdbStats[stat]);
  }
};

exports.init = function (startupTime, config, events, logger) {
  var kairosdb = new KairosdbBackend(startupTime, config, events, logger);

  return true;
};
