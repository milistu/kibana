/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import {
  ORCA_BENCH_GCS_BASE_PATH_PREFIX,
  ORCA_BENCH_GCS_BUCKET,
  ORCA_BENCH_NAMESPACE,
} from '../constants';
import type { DatasetConfig } from './types';

/**
 * ORCA-bench (https://github.com/ORCA-bench/ORCA-bench): six days of OpenTelemetry Demo
 * telemetry with 42 feature-flag incidents, converted to OTel-native data streams and published
 * as one snapshot repository with a named snapshot per incident, per day, and `full`.
 * See elastic/nightshift-program#1271 and the README next to the snapshots in GCS.
 *
 * Logs live in `.ds-logs-orcabench.otel-<yyyy-mm-dd-hh>-*` backing indices, so the
 * `managed-stream` replay picks them up and reindexes them into the `logs` stream. The
 * snapshots run on Docker Compose, not Kubernetes: filter on
 * `resource.attributes.service.name`, never on `resource.attributes.app`. Use `match_phrase`
 * rather than `term` for it: the managed stream maps the field as `text`, so a `term` on a
 * hyphenated name such as `frontend-proxy` never matches.
 */
export const orcaBenchDataset: DatasetConfig = {
  id: ORCA_BENCH_NAMESPACE,
  description:
    'ORCA-bench: OpenTelemetry Demo with SRE-curated feature-flag incidents (logs and traces)',
  optIn: true,
  gcs: {
    bucket: ORCA_BENCH_GCS_BUCKET,
    basePathPrefix: ORCA_BENCH_GCS_BASE_PATH_PREFIX,
    runScoped: false,
  },
  replayMode: 'managed-stream',
  kiFeatureExtraction: [
    {
      input: {
        // Snapshot `incident-d1-i2-adfailure-on`: 2026-04-19 16:00–21:00 UTC. Contains the
        // adFailure incident (16:32–20:02, ad service throws gRPC UNAVAILABLE on ~10% of GetAds)
        // and, from 20:33, the start of kafkaQueueProblems (fraud-detection consumer sleeps).
        scenario_id: 'incident-d1-i2-adfailure-on',
      },
      output: {
        criteria: [
          {
            id: 'entity-frontend-proxy',
            text: 'Must identify the frontend-proxy (Envoy) as an entity, with a filter on resource.attributes.service.name=frontend-proxy; its documents are Envoy access logs (event_name=proxy.access) with request lines like "GET /api/products/... HTTP/1.1" in body.text',
            score: 2,
            sampling_filters: [
              {
                bool: {
                  filter: [
                    { match_phrase: { 'resource.attributes.service.name': 'frontend-proxy' } },
                    { match_phrase: { event_name: 'proxy.access' } },
                  ],
                },
              },
            ],
          },
          {
            id: 'entity-cart',
            text: 'Must identify the cart service as an entity with a filter on resource.attributes.service.name=cart (evidence: .NET logs such as "GetCartAsync called with userId=...", "AddItemAsync called with userId=...")',
            score: 1,
            sampling_filters: [{ match_phrase: { 'resource.attributes.service.name': 'cart' } }],
          },
          {
            id: 'entity-checkout',
            text: 'Must identify the checkout service as an entity with a filter on resource.attributes.service.name=checkout (evidence: "order confirmation email sent to ...", "payment went through", Kafka producer messages "Successful to write message")',
            score: 2,
            sampling_filters: [
              {
                bool: {
                  filter: [
                    { match_phrase: { 'resource.attributes.service.name': 'checkout' } },
                    { match_phrase: { 'body.text': 'order confirmation email sent' } },
                  ],
                },
              },
            ],
          },
          {
            id: 'entity-kafka',
            text: 'Must identify Kafka as an entity or technology with a filter on resource.attributes.service.name=kafka (evidence: broker logs about KRaft snapshots, ProducerStateManager, partition offsets)',
            score: 1,
            sampling_filters: [{ match_phrase: { 'resource.attributes.service.name': 'kafka' } }],
          },
          {
            id: 'entity-ad',
            text: 'Must identify the ad service as an entity with a filter on resource.attributes.service.name=ad (evidence: Java logs from scope oteldemo.AdService, e.g. "received ad request", "GetAds Failed with status ...")',
            score: 1,
            sampling_filters: [{ match_phrase: { 'resource.attributes.service.name': 'ad' } }],
          },
          {
            id: 'dependency-checkout-kafka-consumers',
            text: 'Should identify the order flow through Kafka as a dependency: checkout publishes orders ("Successful to write message"), accounting and fraud-detection consume them ("Consumed record with orderId: ...", "Order details: ...")',
            score: 2,
            sampling_filters: [
              { match_phrase: { 'body.text': 'Consumed record with orderId' } },
              {
                bool: {
                  filter: [
                    { match_phrase: { 'resource.attributes.service.name': 'checkout' } },
                    { match_phrase: { 'body.text': 'Successful to write message' } },
                  ],
                },
              },
            ],
          },
          {
            id: 'failure-ad-getads-unavailable',
            text: 'Must capture the ad-service failure signature: WARN logs "GetAds Failed with status Status{code=UNAVAILABLE ...}" from the ad service, and the matching frontend-proxy access logs for GET /api/data returning HTTP 500 (via_upstream)',
            score: 2,
            sampling_filters: [
              { match_phrase: { 'body.text': 'GetAds Failed' } },
              {
                bool: {
                  filter: [
                    { match_phrase: { 'resource.attributes.service.name': 'frontend-proxy' } },
                    { match_phrase: { 'body.text': '500 - via_upstream' } },
                  ],
                },
              },
            ],
          },
          {
            id: 'failure-kafka-queue-problems',
            text: 'Should capture the Kafka consumer degradation signature: fraud-detection logging "FeatureFlag \'kafkaQueueProblems\' is enabled, sleeping 1 second" and accounting logging "Order parsing failed:" at Error severity',
            score: 1,
            sampling_filters: [
              { match_phrase: { 'body.text': 'kafkaQueueProblems' } },
              { match_phrase: { 'body.text': 'Order parsing failed' } },
            ],
          },
        ],
        min_features: 6,
        max_features: 25,
        required_types: ['entity'],
        expect_entity_filters: true,
        expected_ground_truth:
          'entities=[frontend-proxy, frontend, cart, checkout, kafka, ad, accounting, fraud-detection, product-catalog, product-reviews, payment, shipping, currency, recommendation, quote, email, load-generator], deps=[checkout->kafka, kafka->accounting, kafka->fraud-detection, frontend->ad, frontend-proxy->frontend], infra=[docker compose host otel-demo-prod-01, otel-collector (otelcol-contrib) for log aggregation], error_signatures=[ad: GetAds Failed with status UNAVAILABLE (gRPC 14); frontend-proxy: GET /api/data 500 via_upstream; fraud-detection: kafkaQueueProblems flag enabled, sleeping 1 second; accounting: Order parsing failed]',
      },
      metadata: {
        difficulty: 'medium',
        failure_domain: 'ad',
        failure_mode:
          'adFailure feature flag (gRPC UNAVAILABLE on ~10% of GetAds), plus kafkaQueueProblems onset',
      },
    },
  ],
  kiQueryGeneration: [],
  kiFeatureExclusion: [],
  kiFeatureDeduplication: [],
  discovery: [],
};
