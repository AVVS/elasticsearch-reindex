var async         = require('neo-async'),
    _             = require('lodash'),
    Promise       = require('bluebird'),
    EventEmitter  = require('events').EventEmitter,
    inherits      = require('util').inherits;

function Indexer () {
  EventEmitter.call(this);
};

inherits(Indexer, EventEmitter);

Indexer.prototype.no_bulk = function (docs, options, cb) {
  if (!docs) {
    return cb();
  }

  var self = this;
  options = options || {
    // should eq sockets length
    concurrency: require('os').cpus().length
  };

  var indexer = options.indexer;
  var client = options.client;

  return Promise.map(docs, function (doc) {
    var transformedDoc = indexer(doc);
    var metadata = transformedDoc[0];
    var source = transformedDoc[1];

    return client.create({
      index: options.index,
      type: options.type,
      id: metadata.index._id,
      body: source,
      ignore: 409
    }).tap(function () {
      self.emit('batch-complete', 1);
    });

  }, { concurrency: options.concurrency })
  .nodeify(cb);

};

Indexer.prototype.index = function (docs, options, cb) {
  if (!docs) {
    return cb();
  }

  var self = this;
  options = options || {
    // this is useless, 1 CPU is used, unless we use cluster, each limit is basically queueing bulk IO
    // this should be tuned to the throughput of ES node and 1 core, on which node would operate
    concurrency:require('os').cpus().length,
    // this should be tuned based on the cluster as well
    bulk: 100
  };

  options.indexer = options.indexer || function (item) {
    return [
      { index: { _id: item._id } },
      item._source
    ];
  };

  var bulkSuze = options.bulk;
  function groupByChunks(item, index) {
    return Math.floor(index/bulkSuze);
  }

  var chunks =  _(docs).groupBy(groupByChunks).toArray().value();
  var client = options.client;

  async.eachLimit(chunks, options.concurrency, function indexBatch(chunk, next) {
    var bulk_data = [];

    chunk.forEach(function (item) {
      bulk_data.push.apply(bulk_data, options.indexer(item, options));
    });

    var currentTry = 0;

    setImmediate(function indexData() {
      var request = _.pick(options, ['index', 'type']);
      request.body = bulk_data;

      client.bulk(request, function bulkResponse(err, res) {
        if (err) {
          if (++currentTry > 5) {
            console.error('Failed at:', JSON.stringify(request));
            self.emit('error', err);
            return next(err);
          }

          // delay 5s
          self.emit('error', new Error('request timedout, retry ' + currentTry + ' scheduled'));
          return setTimeout(indexData, 30000);
        }

        self.emit('batch-complete', chunk.length);
        next();

      });

    });

  }, cb);

};

module.exports = Indexer;
