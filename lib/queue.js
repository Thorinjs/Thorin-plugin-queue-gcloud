'use strict';
const PubSub = require('@google-cloud/pubsub');

/**
 * Initializes a GCloud PubSub instance, using the given information
 * */
module.exports = (thorin, opt) => {
  const logger = thorin.logger(opt.logger);
  const credentials = opt.credentials;
  const queueObj = {};
  let popt = {
    credentials: opt.credentials
  };
  if (opt.projectId) popt.projectId = opt.projectId;
  let pubSubObj = new PubSub(popt);
  let topicObj = pubSubObj.topic(opt.topic);
  let subscriptionMap = {};
  let publisherObj;
  /**
   * Performs a PUSH using our pubsub
   * */
  queueObj.push = async (payload, _opt) => {
    if (typeof _opt !== 'object' || !_opt) _opt = {};
    if (payload instanceof Array) {
      let calls = [];
      for (let i = 0, len = payload.length; i < len; i++) {
        let p = payload[i];
        calls.push(queueObj.push(p, _opt));
      }
      return Promise.all(calls);
    }
    if (!publisherObj) {
      if (opt.options.publisher) {
        _opt = thorin.util.extend(_opt, opt.options.publisher);
      }
      publisherObj = topicObj.publisher(_opt);
    }
    if (payload === null || typeof payload === 'undefined') return false;
    let attributes = {};
    if (typeof payload === 'object' && payload) {
      if (typeof payload.attributes === 'object' && payload.attributes) {
        attributes = payload.attributes;
        delete payload.attributes;
      }
    }
    try {
      payload = JSON.stringify(payload);
    } catch (e) {
      logger.warn(`Could not stringify push payload`);
      throw thorin.error('QUEUE.PUSH', 'Message to be published could not be serialized');
    }
    let dt = Buffer.from(payload, 'utf8');
    try {
      return await publisherObj.publish(dt, attributes);
    } catch (e) {
      logger.warn(`Could not publish message to ${opt.topic}`);
      logger.debug(e);
      throw thorin.error('QUEUE.PUSH', 'Message could not be published', e);
    }
  }

  /**
   * Registers a callback function to be executed when pulling messages.
   * */
  queueObj.pull = async (callback, _opt) => {
    if (typeof callback !== 'function') throw thorin.error('QUEUE.PULL', 'A callback function is required when pulling messages');
    if (typeof _opt !== 'object' || !_opt) _opt = {};
    let subscriptionName = _opt.subscription || opt.subscription;
    if (!subscriptionName) throw thorin.error('QUEUE.PULL', 'A valid subscription name is required');
    let fullSubscriptionName = subscriptionName;
    if (opt.topic.indexOf('projects/') === 0 && fullSubscriptionName.indexOf('projects/') !== 0) {
      fullSubscriptionName = opt.topic.split('projects/')[1].split('/')[0];
      fullSubscriptionName = `projects/${fullSubscriptionName}/subscriptions/${subscriptionName}`;
    }
    let subscriptionObj = subscriptionMap[subscriptionName];
    if (!subscriptionObj) {
      try {
        let subscriptions = await topicObj.getSubscriptions();
        for (let i = 0, len = subscriptions.length; i < len; i++) {
          let sub = subscriptions[i][0];
          if (typeof sub !== 'object' || !sub) continue;
          let baseName = sub.name;
          if (baseName === fullSubscriptionName || baseName.split('/').pop() === subscriptionName) {
            subscriptionObj = sub;
            if (typeof opt.messages === 'number') {
              subscriptionObj.flowControl.maxMessages = opt.messages;
            }
            subscriptionMap[subscriptionName] = subscriptionObj;
            break;
          }
        }
      } catch (e) {
        logger.warn(`Could not fetch all existing subscriptions for: ${opt.topic} on pull()`);
        throw thorin.error('QUEUE.PULL', 'An error occurred while preparing subscription connection', e);
      }
    }
    if (!subscriptionObj) {
      try {
        subscriptionObj = await topicObj.createSubscription(subscriptionName, opt.options.subscription || {});
        if (typeof opt.messages === 'number') {
          subscriptionObj.flowControl.maxMessages = opt.messages;
        }
        subscriptionMap[subscriptionName] = subscriptionObj;
      } catch (e) {
        logger.warn(`Could not create subscription ${subscriptionName} for: ${opt.topic}`);
        throw thorin.error('QUEUE.PULL', 'An error occurred while preparing subscription connection', e);
      }
    }
    subscriptionObj.on('message', handleSubscriptionMessage(callback));
    return subscriptionObj;
  }

  function handleSubscriptionMessage(fn) {
    return (msg) => {
      let data = null;
      try {
        data = Buffer.from(msg.data, 'utf8');
      } catch (e) {
        logger.warn(`Could not parse subscription message for: ${msg.id}`);
        logger.debug(e);
        return msg.ack();
      }
      if (data === null) return msg.ack();
      try {
        data = JSON.parse(data);
      } catch (e) {
        logger.warn(`Could not parse subscription message for: ${msg.id}`);
        logger.debug(e);
        return msg.ack();
      }
      msg.data = data;
      fn(msg);
    }
  }

  return queueObj;
}
