'use strict';
const PubSub = require('@google-cloud/pubsub'),
  path = require('path'),
  fs = require('fs'),
  initQueue = require('./lib/queue');
/**
 * The queue system uses Google Cloud PubSub and exposes 3 functions:
 * - push(payload, _opt)
 * - pull(callbackFn, _opt)
 *
 * Notes:
 * - Functions are promisified
 * Example:
 *
 * const queueObj = thorin.plugin('queue');
 * await queueObj.push({
 *  my: 'data'
 * });
 *
 * await queueObj.pull(async (msg) => {
 *  console.log("message", msg);
 *  msg.ack();  // on completed process
 * });
 *
 * NOTE:
 * Credentials can be provided as follow:
 *  - environment variable GOOGLE_APPLICATION_CREDENTIALS containing the JSON credentials (as string)
 *  - actual JSON content of the credentials.json file
 *  - absolute path of the credentials.json file
 *  - path relative to thorin.root of the credentials.json file.
 * */
module.exports = function (thorin, opt, pluginName) {
  const defaultOpt = {
    logger: pluginName || 'queue',
    topic: null,
    projectId: null,
    credentials: null,  // The credentials JSON containing the .json file
    subscription: 'default',  // the subscription name to use
    options: {
      publisher: null, // additional options when creating publisher
      subscription: null  // additional options when creating subscriber
    },
    messages: 1  // max number of messages to pull once.
  };
  opt = thorin.util.extend(defaultOpt, opt);
  let logger = thorin.logger(opt.logger);
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS && (!opt.credentials || (typeof opt.credentials === 'object' && Object.keys(opt.credentials).length === 0))) {
    opt.credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  }
  if (!opt.credentials) {
    logger.fatal(`PubSub GCloud credentials are missing`);
    throw thorin.error('QUEUE.CREDENTIALS', 'Invalid or missing credentials');
  }
  if (typeof opt.credentials === 'string' && opt.credentials) {
    opt.credentials = opt.credentials.trim();
    if (opt.credentials.charAt(0) === '{') {
      try {
        opt.credentials = JSON.parse(opt.credentials);
      } catch (e) {
        throw thorin.error('QUEUE.GCLOUD', 'Credentials could not be parsed');
      }
    } else {
      let credPath = opt.credentials.charAt(0) === '/' ? path.normalize(opt.credentials) : path.normalize(thorin.root + '/' + opt.credentials);
      try {
        let creds = fs.readFileSync(credPath, {encoding: 'utf8'});
        creds = JSON.parse(creds);
        opt.credentials = creds;
      } catch (e) {
        throw thorin.error('QUEUE.GCLOUD', 'Credentials could not be read [' + credPath + ']');
      }
    }
  }
  if (!opt.topic) throw thorin.error('QUEUE.GCLOUD', 'The topic name is required');
  let queueMap = {};
  let queueObj = initQueue(thorin, opt);
  queueObj.id = thorin.util.randomString(5);
  /**
   * Creates a new queue
   * */
  queueObj.create = (_opt, name) => {
    if (typeof _opt !== 'object' || !_opt) _opt = {};
    _opt = thorin.util.extend(opt, _opt);
    let qObj = initQueue(thorin, _opt);
    qObj.id = thorin.util.randomString(4);
    if (name) {
      queueMap[name] = qObj;
    }
    return qObj;
  }

  /**
   * RETURN a previously created queue
   * */
  queueObj.get = (name) => {
    return queueMap[name] || null;
  };

  return queueObj;
};
module.exports.publicName = 'queue';