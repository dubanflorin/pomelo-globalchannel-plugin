var utils = require('../util/utils');
var redis = require('redis');

var DEFAULT_PREFIX = 'POMELO:CHANNEL';
var DEFAULT_PLAYER_CHANNELS_PREFIX = 'POMELO:PLAYER_CHANNELS';

var GlobalChannelManager = function(app, opts) {
  this.app = app;
  this.opts = opts || {};
  this.prefix = opts.prefix || DEFAULT_PREFIX;
  this.playerChannelsPrefix = opts.playerChannelsPrefix || DEFAULT_PLAYER_CHANNELS_PREFIX;
  this.host = opts.host;
  this.port = opts.port;
  this.db = opts.db || '0';
  this.redis = null;
};

module.exports = GlobalChannelManager;

GlobalChannelManager.prototype.start = function(cb) {
  this.redis = redis.createClient(this.port, this.host, this.opts);
  if (this.opts.auth_pass) {
    this.redis.auth(this.opts.auth_pass);
  }
  var self = this;
  this.redis.on("error", function (err) {
      console.error("[globalchannel-plugin][redis]" + err.stack);
  });
  this.redis.once('ready', function(err) {
    if (!!err) {
      cb(err);
    } else {
      self.redis.select(self.db, cb);
    }
  });
};

GlobalChannelManager.prototype.stop = function(force, cb) {
  if(this.redis) {
    this.redis.end();
    this.redis = null;
  }
  utils.invokeCallback(cb);
};

GlobalChannelManager.prototype.clean = function(cb) {
  var cmds = [];
  var self = this;
  this.redis.keys(genCleanKey(this), function(err, list) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    for(var i=0; i<list.length; i++) {
      cmds.push(['del', list[i]]);
    }

    self.redis.keys(genPlayerChannelsCleanKey(self), function (err, list) {
      if(!!err) {
        utils.invokeCallback(cb, err);
        return;
      }
      for(var i=0; i<list.length; i++) {
        cmds.push(['del', list[i]]);
      }
      execMultiCommands(self.redis, cmds, cb);
    });
  });
};

GlobalChannelManager.prototype.destroyChannel = function(name, cb) {
  var servers = this.app.getServers();
  var server, cmds = [];
  var self = this;

  for(var sid in servers) {
    server = servers[sid];
    if(this.app.isFrontend(server)) {
      cmds.push(['del', genKey(this, name, sid)]);
    }
  }

  self.redis.keys(genPlayerChannelsCleanKey(self), function (err, list) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }

    for(var i=0; i<list.length; i++) {
      cmds.push(['srem', list[i], name]);
    }

    execMultiCommands(self.redis, cmds, cb);
  });
};

GlobalChannelManager.prototype.add = function(name, uid, sid, cb) {
  var commands = [];

  commands.push(['sadd', genKey(this, name, sid), uid]);
  commands.push(['sadd', genPlayerChannelsKey(this, uid, sid), name]);

  execMultiCommands(this.redis, commands, cb);
};

GlobalChannelManager.prototype.leave = function(name, uid, sid, cb) {
  var commands = [];

  commands.push(['srem', genKey(this, name, sid), uid]);
  commands.push(['srem', genPlayerChannelsKey(this, uid, sid), name]);

  execMultiCommands(this.redis, commands, cb);
};

GlobalChannelManager.prototype.leaveAllChannels = function (uid, sid, cb) {
  var self = this;
  var commands = [];

  self.redis.smembers(genPlayerChannelsKey(self, uid, sid), function (err, list) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }

    for(var i=0; i<list.length; i++) {
      commands.push(['srem', genKey(this, list[i], sid), uid]);
    }

    commands.push(['del', genPlayerChannelsKey(self, uid, sid)]);

    execMultiCommands(self.redis, commands, cb);
  });
};

GlobalChannelManager.prototype.getMembersBySid = function(name, sid, cb) {
  this.redis.smembers(genKey(this, name, sid), function(err, list) {
    utils.invokeCallback(cb, err, list);
  });
};

var execMultiCommands = function(redis, cmds, cb) {
  if(!cmds.length) {
    utils.invokeCallback(cb);
    return;
  }
  redis.multi(cmds).exec(function(err, reply) {
    utils.invokeCallback(cb, err);
  });
};

var genKey = function(self, name, sid) {
  return self.prefix + ':' + name + ':' + sid;
};

var genCleanKey = function(self) {
  return self.prefix + '*' + self.app.serverId;
};

var genPlayerChannelsKey = function (self, uid, sid) {
  return self.playerChannelsPrefix + ':' + uid + ':' + sid;
};

var genPlayerChannelsCleanKey = function (self) {
  return self.playerChannelsPrefix + ':*:' + self.app.serverId;
};
