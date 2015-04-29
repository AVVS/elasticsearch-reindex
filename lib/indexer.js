var async         = require('neo-async'),
    _             = require('lodash'),
    EventEmitter  = require('events').EventEmitter,
    inherits      = require('util').inherits;

function Indexer () {
  EventEmitter.call(this);
};

inherits(Indexer, EventEmitter);

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
            self.emit('error', err);
            return next(err);
          }

          // delay 5s
          self.emit('error', new Error('request timedout, retry ' + currentTry + ' scheduled'));
          return setTimeout(indexData, 30000);
        }

        if (res.errors) {
          res.items.forEach(function (item) {
            if (_.indexOf([200, 201], item.index.status) == -1) {
              self.emit('item-failed', item);
            }
            self.emit('batch-complete', 1);
          });
        } else {
          self.emit('batch-complete', chunk.length);
        }

        next();

      });

    });

  }, cb);

};

module.exports = Indexer;
