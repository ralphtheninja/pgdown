'use strict'

const test = require('tape')
const common = require('./_common')
const PgDOWN = require('../')

// TODO: use a larger buffer
const buffer = new Buffer('00ff61626301feffff00000000ffff', 'hex')

// verify compatibility w/ leveldown api
const suites = {
  leveldown: require('abstract-leveldown/abstract/leveldown-test').args,
  open: require('abstract-leveldown/abstract/open-test').all,
  close: require('abstract-leveldown/abstract/close-test').close,
  approximateSize: require('abstract-leveldown/abstract/approximate-size-test').all,
  put: require('abstract-leveldown/abstract/put-test').all,
  del: require('abstract-leveldown/abstract/del-test').all,
  get: require('abstract-leveldown/abstract/get-test').all,
  put_get_del: require('abstract-leveldown/abstract/put-get-del-test').all,
  iterator: require('abstract-leveldown/abstract/iterator-test').all,
  ranges: require('abstract-leveldown/abstract/ranges-test').all,
  batch: require('abstract-leveldown/abstract/batch-test').all,
  chainedBatch: require('abstract-leveldown/abstract/chained-batch-test').all
}

const factories = [ PgDOWN ]

factories.forEach((factory) => {
  Object.keys(suites).forEach((name) => {
    suites[name](factory, test, common, buffer)
  })
})
