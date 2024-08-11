/**
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from "assert";
import fs from "fs";
import path from "path";

import { AdsQueryExecutor } from "../lib/ads-query-executor";
import {
  JsonOutputFormat,
  JsonValueFormat,
  JsonWriter,
} from "../lib/file-writers";

import { MockGoogleAdsApiClient } from "./helpers";

suite("JsonWriter", () => {
  let executor: AdsQueryExecutor;
  const OUTPUT_DIR = ".tmp";
  const SCRIPT_NAME = "test";

  let queryText = `
      SELECT
        ad_group_ad.ad.id AS ad_id,  --number
        ad_group_ad.ad.final_urls AS final_urls,  -- array<string>
        ad_group_ad.ad.type AS ad_type,  -- enum
        ad_group_ad.ad_group AS ad_group,  -- resource_name
        ad_group_ad.policy_summary.policy_topic_entries AS policy_topic_entries  -- array
      FROM ad_group_ad
    `;

  let customers = ["cust_with_no_data", "cust_with_data"];
  let mock_result = [
    {
      campaign: {
        id: 1767375787,
        resource_name: "customers/9489090398/campaigns/1767375787",
      },
      ad_group_ad: {
        ad: {
          id: 563386468726,
          final_urls: ["url1", "url2"],
          type: 7,
          resource_name: "customers/9489090398/ads/563386468726",
        },
        ad_group: "customers/9489090398/adGroups/132594495320",
        policy_summary: {
          policy_topic_entries: [
            {
              evidences: [],
              constraints: [{}],
              topic: "COPYRIGHTED_CONTENT",
              type: 8,
            },
          ],
        },
        resource_name:
          "customers/9489090398/adGroupAds/132594495320~563386468726",
      },
    },
    // 2
    {
      campaign: {
        id: 2222222222,
        resource_name: "customers/9489090398/campaigns/2222222222",
      },
      ad_group_ad: {
        ad: {
          id: 123456789012,
          final_urls: ["url1", "url2"],
          type: 7,
          resource_name: "customers/9489090398/ads/123456789012",
        },
        ad_group: "customers/9489090398/adGroups/132594495320",
        policy_summary: {
          policy_topic_entries: [
            {
              evidences: [],
              constraints: [{}],
              topic: "COPYRIGHTED_CONTENT",
              type: 8,
            },
          ],
        },
        resource_name:
          "customers/9489090398/adGroupAds/132594495320~123456789012",
      },
    },
  ];

  setup(() => {
    let client = new MockGoogleAdsApiClient();
    client.setupResult({ cust_with_data: mock_result });
    executor = new AdsQueryExecutor(client);
  });

  function getJson() {
    let jsonText = fs.readFileSync(
      path.join(OUTPUT_DIR, SCRIPT_NAME + ".json"),
      "utf-8"
    );
    console.log(jsonText);
    let json = JSON.parse(jsonText);
    console.log(json);
    assert(json);
    return json;
  }

  test("writing in json (format=json) with valueFormat=object", async function () {
    // arrange
    let writer = new JsonWriter({
      outputPath: OUTPUT_DIR,
      format: JsonOutputFormat.json,
      valueFormat: JsonValueFormat.objects,
    });

    // act
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    const json = getJson();
    assert.equal(json.length, 2);
    let row = json[0];
    assert.equal(row.ad_id, mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(
      row.final_urls,
      mock_result[0].ad_group_ad.ad.final_urls
    );
  });

  test("writing in json (format=json) with valueFormat=arrays", async function () {
    // arrange
    let writer = new JsonWriter({
      outputPath: OUTPUT_DIR,
      format: JsonOutputFormat.json,
      valueFormat: JsonValueFormat.arrays,
    });

    // act
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    const json = getJson();
    assert.equal(json.length, 3);
    assert.deepEqual(json[0], [
      "ad_id",
      "final_urls",
      "ad_type",
      "ad_group",
      "policy_topic_entries",
    ]);
    let row = json[1];
    assert.equal(row[0], mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(row[1], mock_result[0].ad_group_ad.ad.final_urls);
  });

  test("writing in json (format=json) with valueFormat=raw", async function () {
    // arrange
    let writer = new JsonWriter({
      outputPath: OUTPUT_DIR,
      format: JsonOutputFormat.json,
      valueFormat: JsonValueFormat.raw,
    });

    // act
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    const json = getJson();
    assert.equal(json.length, 2);
    let row = json[0];
    assert.equal(row.ad_group_ad.ad.id, mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(
      row.ad_group_ad.ad.final_urls,
      mock_result[0].ad_group_ad.ad.final_urls
    );
  });

  test("writing in jsonl with valueFormat=object", async function () {
    // arrange
    let writer = new JsonWriter({
      outputPath: OUTPUT_DIR,
      format: JsonOutputFormat.jsonl,
      valueFormat: JsonValueFormat.objects,
    });

    // act
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    let jsonText = fs.readFileSync(
      path.join(OUTPUT_DIR, SCRIPT_NAME + ".json"),
      "utf-8"
    );
    console.log(jsonText);
    const lines = jsonText.split("\n").filter((s) => s.length > 0);
    assert.equal(lines.length, 2);
    let row = JSON.parse(lines[0]);
    assert.equal(row.ad_id, mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(
      row.final_urls,
      mock_result[0].ad_group_ad.ad.final_urls
    );
  });
});
