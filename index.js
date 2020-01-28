const Minio = require('minio');
const AWS = require('aws-sdk');
const shell = require('shelljs');
const path = require('path');
const { fromEvent } = require('rxjs');
const functionHelper = require('serverless-offline/src/functionHelper');
const LambdaContext = require('serverless-offline/src/LambdaContext');

const defaultOptions = {
  port: 4569,
  address: 'localhost',
  location: '.',
  accessKeyId: 'S3RVER',
  secretAccessKey: 'S3RVER',
};

const removeBucket = ({ bucket, port, host }) => new Promise((resolve, reject) => {
  shell.exec(
    `aws --endpoint http://${host}:${port} s3 rb "s3://${bucket}" --force`,
    { silent: true },
    (code, stdout, stderr) => {
      if (code === 0) return resolve();
      if (stderr && stderr.indexOf('NoSuchBucket') !== -1) return resolve();

      return reject(
        new Error(`failed to delete bucket ${bucket}: ${stderr || stdout}`),
      );
    },
  );
});

class ServerlessS3Local {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.options = options;
    this.provider = 'aws';
    this.client = null;

    this.commands = {
      s3: {
        commands: {
          start: {
            usage: 'Start S3 local server.',
            lifecycleEvents: ['startHandler'],
            options: {
              port: {
                shortcut: 'p',
                usage:
                  'The port number that S3 will use to communicate with your application. If you do not specify this option, the default port is 4569',
              },
              buckets: {
                shortcut: 'b',
                usage: 'After starting S3 local, create specified buckets',
              },
            },
          },
          create: {
            usage: 'Create local S3 buckets.',
            lifecycleEvents: ['createHandler'],
            options: {
              port: {
                shortcut: 'p',
                usage:
                  'The port number that S3 will use to communicate with your application. If you do not specify this option, the default port is 4569',
              },
              buckets: {
                shortcut: 'b',
                usage: 'After starting S3 local, create specified buckets',
              },
            },
          },
          remove: {
            usage: 'Remove local S3 buckets.',
            lifecycleEvents: ['createHandler'],
            options: {
              port: {
                shortcut: 'p',
                usage:
                  'The port number that S3 will use to communicate with your application. If you do not specify this option, the default port is 4569',
              },
              buckets: {
                shortcut: 'b',
                usage: 'After starting S3 local, create specified buckets',
              },
            },
          },
        },
      },
    };

    this.hooks = {
      's3:start:startHandler': this.startHandler.bind(this),
      's3:create:createHandler': this.createHandler.bind(this),
      's3:remove:createHandler': this.removeHandler.bind(this),
      'before:offline:start:init': this.startHandler.bind(this),
      'before:offline:start': this.startHandler.bind(this),
      'after:webpack:compile:watch:compile': this.subscriptionWebpackHandler.bind(
        this,
      ),
    };
  }

  subscriptionWebpackHandler() {
    return new Promise((resolve) => {
      if (!this.s3eventSubscription) {
        resolve();
      }

      this.s3eventSubscription.forEach((subscription) => subscription.unsubscribe());

      this.subscribe();
      console.log('constructor');
      resolve();
    });
  }

  subscribe() {
    if (
      typeof this.service !== 'object'
      || typeof this.service.functions !== 'object'
    ) {
      return {};
    }

    const servicePath = path.join(
      this.serverless.config.servicePath,
      this.options.location,
    );
    const serviceRuntime = this.getServiceRuntime();

    Object.keys(this.service.functions).forEach((key) => {
      const serviceFunction = this.service.getFunction(key);

      const lambdaContext = new LambdaContext(
        serviceFunction,
        this.service.provider,
        (err, res) => {
          if (err) {
            console.error(err);
          }
          if (res) {
            this.serverless.cli.log(res);
          }
        },
      );
      const funOptions = functionHelper.getFunctionOptions(
        serviceFunction,
        key,
        servicePath,
        serviceRuntime,
      );

      const func = (s3Event) => {
        const baseEnvironment = {
          IS_LOCAL: true,
          IS_OFFLINE: true,
        };

        try {
          Object.assign(
            process.env,
            baseEnvironment,
            this.service.provider.environment,
            serviceFunction.environment || {},
          );

          const handler = functionHelper.createHandler(
            funOptions,
            this.options,
          );
          const callback = (error, response) => {
            console.log(
              `serverless-s3-minio-local: callback is called with ${error} and ${response}`,
            );
          };
          handler(s3Event, lambdaContext, callback);
        } catch (e) {
          console.error('Error while running handler', e);
        }
      };

      serviceFunction.events.forEach((event) => {
        const s3 = (event && (event.s3 || event.existingS3)) || undefined;
        if (!s3) {
          return;
        }

        const handlerBucketName = typeof s3 === 'object' ? s3.bucket : s3;
        const bucketResource = this.getResourceForBucket(handlerBucketName);
        const name = bucketResource
          ? bucketResource.Properties.BucketName
          : handlerBucketName;
        const s3Events = s3.events ? s3.events : [s3.event];
        const rules = s3.rules || [];
        const { suffix } = rules.find((e) => e.suffix) || { suffix: '' };
        const { prefix } = rules.find((e) => e.prefix) || { prefix: '' };

        this.serverless.cli.log(`Found S3 event listener for ${name}, function: ${key}, prefix: ${prefix}, suffix: ${suffix}`);

        const eventEmitter = this.client.listenBucketNotification(name, prefix, suffix, s3Events);
        const observable = fromEvent(eventEmitter, 'notification');
        const subscription = observable.subscribe((s3Event) => {
          func({ Records: [s3Event] });
        });
        this.s3eventSubscription = [];
        this.s3eventSubscription.push(subscription);
      });
    });
  }

  async startHandler() {
    try {
      this.setOptions();
      await this.createBuckets();
      const {
        host,
        port,
        accessKeyId,
        secretAccessKey,
      } = this.options;
      this.client = new Minio.Client({
        endPoint: host,
        port,
        useSSL: false,
        accessKey: accessKeyId,
        secretKey: secretAccessKey,
      });
      this.subscribe();
    } catch (e) {
      console.error('Failed to start s3 local', e);
    }
  }

  createHandler() {
    this.setOptions();
    return this.createBuckets();
  }

  removeHandler() {
    this.setOptions();
    return this.removeBuckets();
  }

  createBuckets() {
    const buckets = this.buckets();
    if (!buckets.length) {
      console.log('WARN: No buckets found to create');
      return Promise.resolve([]);
    }

    const s3Client = this.getClient();
    return Promise.all(
      buckets.map((Bucket) => {
        this.serverless.cli.log(`creating bucket: ${Bucket}`);
        return s3Client.createBucket({ Bucket }).promise();
      }),
    ).catch(() => ({}));
  }

  removeBuckets() {
    return Promise.resolve().then(() => {
      const { port, host } = this.options;
      const buckets = this.buckets();
      if (!buckets.length) return null;

      return Promise.all(
        buckets.map((bucket) => {
          this.serverless.cli.log(`removing bucket: ${bucket}`);
          return removeBucket({ port, bucket, host });
        }),
      );
    });
  }

  getClient() {
    return new AWS.S3({
      s3ForcePathStyle: true,
      endpoint: new AWS.Endpoint(
        `http://${this.options.host}:${this.options.port}`,
      ),
      accessKeyId: this.options.accessKeyId,
      secretAccessKey: this.options.secretAccessKey,
    });
  }

  getServiceRuntime() {
    // Following codes are derived from serverless/index.js
    let serviceRuntime = this.service.provider.runtime;

    if (!serviceRuntime) {
      throw new Error('Missing required property "runtime" for provider.');
    }

    if (typeof serviceRuntime !== 'string') {
      throw new Error(
        'Provider configuration property "runtime" wasn\'t a string.',
      );
    }

    if (serviceRuntime === 'provided') {
      if (this.options.providedRuntime) {
        serviceRuntime = this.options.providedRuntime;
      } else {
        throw new Error(
          'Runtime "provided" is unsupported. Please add a --providedRuntime CLI option.',
        );
      }
    }

    if (
      !(
        serviceRuntime.startsWith('nodejs')
        || serviceRuntime.startsWith('python')
        || serviceRuntime.startsWith('ruby')
      )
    ) {
      this.serverless.cli.log(
        `Warning: found unsupported runtime '${serviceRuntime}'`,
      );

      return null;
    }

    return serviceRuntime;
  }

  static buildEventHandler(s3, name, pattern, s3Rules, func) {
    const rule2regex = (rule) => Object.keys(rule).map(
      (key) => (key === 'prefix' && { prefix: `^${rule[key]}` }) || {
        suffix: `${rule[key]}$`,
      },
    );
    const rules = typeof s3 === 'object'
      ? [].concat(...(s3Rules || []).map(rule2regex))
      : [];

    return {
      name,
      pattern,
      rules,
      func,
    };
  }

  getResourceForBucket(bucketName) {
    const logicalResourceName = `S3Bucket${bucketName
      .charAt(0)
      .toUpperCase()}${bucketName.substr(1)}`;
    return this.service.resources && this.service.resources.Resources
      ? this.service.resources.Resources[logicalResourceName]
      : false;
  }

  getAdditionalStacks() {
    const serviceAdditionalStacks = this.service.custom.additionalStacks || {};
    const additionalStacks = [];
    Object.keys(serviceAdditionalStacks).forEach((stack) => {
      additionalStacks.push(serviceAdditionalStacks[stack]);
    });
    return additionalStacks;
  }

  hasPlugin(pluginName, strict = false) {
    return this.service && this.service.plugins && this.service.plugins.modules
      ? this.service.plugins.modules.some((module) => {
        const index = module.indexOf(pluginName);
        return strict ? index === 0 : index >= 0;
      })
      : this.service.plugins.some((plugin) => {
        const index = plugin.indexOf(pluginName);
        return strict ? index === 0 : index >= 0;
      });
  }

  hasAdditionalStacksPlugin() {
    return this.hasPlugin('additional-stacks');
  }

  hasExistingS3Plugin() {
    return this.hasPlugin('existing-s3');
  }

  /**
   * Get bucket list from serverless.yml resources and additional stacks
   *
   * @return {object} Array of bucket name
   */
  buckets() {
    const resources = (this.service.resources && this.service.resources.Resources) || {};
    if (this.hasAdditionalStacksPlugin()) {
      let additionalStacks = [];
      additionalStacks = additionalStacks.concat(this.getAdditionalStacks());
      additionalStacks.forEach((stack) => {
        if (stack.Resources) {
          Object.keys(stack.Resources).forEach((key) => {
            if (stack.Resources[key].Type === 'AWS::S3::Bucket') {
              resources[key] = stack.Resources[key];
            }
          });
        }
      });
    }

    // support for serverless-plugin-existing-s3
    // https://www.npmjs.com/package/serverless-plugin-existing-s3
    if (this.hasExistingS3Plugin()) {
      const { functions } = this.serverless.service;
      const functionNames = Object.keys(functions);
      functionNames.forEach((name) => {
        functions[name].events.forEach((event) => {
          const eventKeys = Object.keys(event);
          // check if the event has an existingS3 and add if the bucket name
          // is not already in the array
          if (eventKeys.indexOf('existingS3') > -1) {
            const resourceName = `LocalS3Bucket${event.existingS3.bucket}`;
            const localBucket = {
              Type: 'AWS::S3::Bucket',
              Properties: {
                BucketName: event.existingS3.bucket,
              },
            };
            resources[resourceName] = localBucket;
          }
        });
      });
    }

    const eventSourceBuckets = Object.keys(this.service.functions).reduce(
      (acc, key) => {
        const serviceFunction = this.service.getFunction(key);
        return acc.concat(
          serviceFunction.events
            .map((event) => {
              const s3 = (event && (event.s3 || event.existingS3)) || undefined;
              if (!s3) {
                return null;
              }

              return typeof s3 === 'object' ? s3.bucket : s3;
            })
            .filter((bucket) => bucket !== null),
        );
      },
      [],
    );

    return Object.keys(resources)
      .map((key) => {
        if (
          resources[key].Type === 'AWS::S3::Bucket'
          && resources[key].Properties
          && resources[key].Properties.BucketName
        ) {
          return resources[key].Properties.BucketName;
        }
        return null;
      })
      .concat(this.options.buckets)
      .concat(eventSourceBuckets)
      .filter((n) => n);
  }

  setOptions() {
    const config = (this.serverless.service.custom && this.serverless.service.custom.s3)
      || {};
    this.options = {
      ...defaultOptions,
      ...(this.service.custom || {})['serverless-offline'],
      ...this.options,
      ...config,
    };
  }
}

module.exports = ServerlessS3Local;
