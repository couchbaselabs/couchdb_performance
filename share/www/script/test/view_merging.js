// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

couchTests.view_merging = function(debug) {
  if (debug) debugger;

  function newDb(name) {
    var db = new CouchDB(name, {"X-Couch-Full-Commit": "false"});
    db.deleteDb();
    db.createDb();

    return db;
  }

  function dbUri(db) {
    return CouchDB.protocol + CouchDB.host + '/' + db.name;
  }

  function populateAlternated(dbs, docs) {
    var docIdx = 0;

    while (docIdx < docs.length) {
      for (var i = 0; (i < dbs.length) && (docIdx < docs.length); i++) {
        var db = dbs[i];
        var doc = docs[docIdx];

        TEquals(true, db.save(doc).ok);
        docIdx += 1;
      }
    }
  }

  function populateSequenced(dbs, listOfDocLists) {
    for (var i = 0, j = 0; (i < dbs.length) && (j < listOfDocLists.length); i++, j++) {
      var db = dbs[i];
      var docList = listOfDocLists[j];

      for (var k = 0; k < docList.length; k++) {
        var doc = docList[k];
        TEquals(true, db.save(doc).ok);
      }
    }
  }

  function addDoc(dbs, doc) {
    for (var i = 0; i < dbs.length; i++) {
      TEquals(true, dbs[i].save(doc).ok);
      delete doc._rev;
    }
  }

  function mergedQuery(dbs, viewname, options) {
    var dbNames = [];
    options = options || {};

    for (var i = 0; i < dbs.length; i++) {
      if (typeof dbs[i] === "string") {
        dbNames.push(dbs[i]);
      } else {
        dbNames.push(dbs[i].name);
      }
    }

    var body = {
      "databases": dbNames,
      "viewname": viewname
    };
    var qs = "";

    for (var q in options) {
      if (q === "keys") {
        body["keys"] = options[q];
        continue;
      }
      if (q === "connection_timeout") {
        body["connection_timeout"] = options[q];
        continue;
      }
      if (q === "on_error") {
        body["on_error"] = options[q];
        continue;
      }
      if (qs !== "") {
        qs = qs + "&";
      }
      qs = qs + String(q) + "=" + String(options[q]);
    }

    if (qs !== "") {
      qs = "?" + qs;
    }

    var xhr = CouchDB.request("POST", "/_view_merge" + qs, {
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });
    TEquals(200, xhr.status);

    return JSON.parse(xhr.responseText);
  }

  function testKeysSorted(resp, direction) {
    direction = direction || "fwd";
    for (var i = 0; i < (resp.rows.length - 1); i++) {
      var row = resp.rows[i];
      var nextRow = resp.rows[i + 1];

      if (direction === "rev") {
        T(row.key >= nextRow.key, "keys are sorted in reverse order");
      } else {
        T(row.key <= nextRow.key, "keys are sorted");
      }
    }
  }

  function wait(ms) {
    var t0 = new Date(), t1;
    do {
      CouchDB.request("GET", "/");
      t1 = new Date();
    } while ((t1 - t0) <= ms);
  }

  function compareViewResults(resultA, resultB) {
    TEquals(resultA.rows.length, resultB.rows.length, "same # of rows");

    for (var i = 0; i < resultA.rows.length; i++) {
      var a = resultA.rows[i];
      var b = resultB.rows[i];

      TEquals(JSON.stringify(a.key), JSON.stringify(b.key), "keys are equal");
      TEquals(JSON.stringify(a.value), JSON.stringify(b.value),
        "values are equal");
    }
  }


  /**
   * Tests with map views.
   */

  var ddoc = {
    _id: "_design/test",
    language: "javascript",
    views: {
      mapview1: {
        map:
          (function(doc) {
             emit(doc.integer, doc.string);
          }).toString()
      },
      redview1: {
        map:
          (function(doc) {
             emit([doc.integer, doc.string], doc.integer);
             emit([doc.integer + 1, doc.string], doc.integer + 1);
          }).toString(),
        reduce:
          (function(keys, values, rereduce) {
             return sum(values);
          }).toString()
      }
    }
  };

  // test with empty dbs
  var dbA, dbB, dbs, docs, resp, resp2, i;
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);

  resp = mergedQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(0, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(0, resp.rows.length);


  // test 1 empty db and one non-empty db
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = [makeDocs(1, 11)];
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateSequenced([dbA], docs);

  resp = mergedQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(10, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);

  testKeysSorted(resp);


  // 2 dbs, alternated keys
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = makeDocs(1, 41);
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateAlternated(dbs, docs);

  resp = mergedQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);

  // same, but with a remote db name
  resp2 = mergedQuery([dbA, dbUri(dbB)], "test/mapview1");

  compareViewResults(resp, resp2);

  // now test stale=ok works
  populateAlternated(dbs, makeDocs(41, 43));

  resp = mergedQuery(dbs, "test/mapview1", {stale: "ok"});

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  // same, but with a remote db name
  resp2 = mergedQuery([dbA, dbUri(dbB)], "test/mapview1", {stale: "ok"});

  compareViewResults(resp, resp2);

  // test stale=update_after works

  resp = mergedQuery(dbs, "test/mapview1", {stale: "update_after"});

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  // wait a bit, the view should now reflect the 2 new documents
  wait(1000);

  resp = mergedQuery(dbs, "test/mapview1", {stale: "ok"});

  TEquals("object", typeof resp);
  TEquals(42, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(42, resp.rows.length);
  TEquals(41, resp.rows[40].key);
  TEquals("41", resp.rows[40].id);
  TEquals(42, resp.rows[41].key);
  TEquals("42", resp.rows[41].id);

  testKeysSorted(resp);

  // 2 dbs, sequenced keys (worst case)
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = [makeDocs(1, 21), makeDocs(21, 41)];
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateSequenced(dbs, docs);

  resp = mergedQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbUri(dbB)], "test/mapview1");

  compareViewResults(resp, resp2);


  // 5 dbs, alternated keys
  var dbC, dbD, dbE;
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbC = newDb("test_db_c");
  dbD = newDb("test_db_d");
  dbE = newDb("test_db_e");
  docs = makeDocs(1, 51);
  dbs = [dbA, dbB, dbC, dbD, dbE];

  addDoc(dbs, ddoc);
  populateAlternated(dbs, docs);

  resp = mergedQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1");

  compareViewResults(resp, resp2);

  // test skip=N query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"skip": 2});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(48, resp.rows.length);
  TEquals(3, resp.rows[0].key);
  TEquals("3", resp.rows[0].id);
  TEquals(4, resp.rows[1].key);
  TEquals("4", resp.rows[1].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"skip": 2});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"skip": 49});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(1, resp.rows.length);
  TEquals(50, resp.rows[0].key);
  TEquals("50", resp.rows[0].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"skip": 49});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"skip": 0});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"skip": 0});

  compareViewResults(resp, resp2);

  // test limit=N query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"limit": 1});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(1, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"limit": 1});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"limit": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(10, resp.rows[9].key);
  TEquals("10", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"limit": 10});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"limit": 1000});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(50, resp.rows[49].key);
  TEquals("50", resp.rows[49].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"limit": 1000});

  compareViewResults(resp, resp2);

  // test skip=N with limit=N query parameters
  resp = mergedQuery(dbs, "test/mapview1", {"limit": 10, "skip": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(11, resp.rows[0].key);
  TEquals("11", resp.rows[0].id);
  TEquals(20, resp.rows[9].key);
  TEquals("20", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"limit": 10, "skip": 10});

  compareViewResults(resp, resp2);

  // test starkey query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"startkey": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(41, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(50, resp.rows[40].key);
  TEquals("50", resp.rows[40].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10});

  compareViewResults(resp, resp2);

  // test starkey query parameter with startkey_docid (same result as before)
  resp = mergedQuery(dbs, "test/mapview1",
      {"startkey": 10, "startkey_docid": "10"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(41, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(50, resp.rows[40].key);
  TEquals("50", resp.rows[40].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "startkey_docid": "10"});

  compareViewResults(resp, resp2);

  // test starkey query parameter with startkey_docid (not same result as before)
  resp = mergedQuery(dbs, "test/mapview1",
      {"startkey": 10, "startkey_docid": "11"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);
  TEquals(11, resp.rows[0].key);
  TEquals("11", resp.rows[0].id);
  TEquals(50, resp.rows[39].key);
  TEquals("50", resp.rows[39].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "startkey_docid": "11"});

  compareViewResults(resp, resp2);

  // test starkey query parameter with limit
  resp = mergedQuery(dbs, "test/mapview1", {"startkey": 10, "limit": 5});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(5, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(14, resp.rows[4].key);
  TEquals("14", resp.rows[4].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "limit": 5});

  compareViewResults(resp, resp2);

  // test starkey query parameter with limit and skip
  resp = mergedQuery(dbs, "test/mapview1", {"startkey": 10, "limit": 5, "skip": 2});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(5, resp.rows.length);
  TEquals(12, resp.rows[0].key);
  TEquals("12", resp.rows[0].id);
  TEquals(16, resp.rows[4].key);
  TEquals("16", resp.rows[4].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "limit": 5, "skip": 2});

  compareViewResults(resp, resp2);

  // test endkey query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"endkey": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(10, resp.rows[9].key);
  TEquals("10", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"endkey": 10});

  compareViewResults(resp, resp2);

  // test endkey query parameter with endkey_docid (same result as before)
  resp = mergedQuery(dbs, "test/mapview1", {"endkey": 10, "endkey_docid": "10"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(10, resp.rows[9].key);
  TEquals("10", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"endkey": 10, "endkey_docid": "10"});

  compareViewResults(resp, resp2);

  // test endkey query parameter with endkey_docid (not same result as before)
  resp = mergedQuery(dbs, "test/mapview1", {"endkey": 10, "endkey_docid": "0"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(9, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(9, resp.rows[8].key);
  TEquals("9", resp.rows[8].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"endkey": 10, "endkey_docid": "0"});

  compareViewResults(resp, resp2);

  // test endkey query parameter with inclusive_end=false
  resp = mergedQuery(dbs, "test/mapview1",
    {"endkey": 10, "inclusive_end": "false"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(9, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(9, resp.rows[8].key);
  TEquals("9", resp.rows[8].id);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"endkey": 10, "inclusive_end": "false"});

  compareViewResults(resp, resp2);

  // test endkey query parameter with limit
  resp = mergedQuery(dbs, "test/mapview1", {"endkey": 10, "limit": 3});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(3, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(3, resp.rows[2].key);
  TEquals("3", resp.rows[2].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"endkey": 10, "limit": 3});

  compareViewResults(resp, resp2);

  // test starkey with endkey query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"startkey": 10, "endkey": 20});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(11, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(20, resp.rows[10].key);
  TEquals("20", resp.rows[10].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "endkey": 20});

  compareViewResults(resp, resp2);

  // test starkey query parameter with descending order
  resp = mergedQuery(dbs, "test/mapview1", {"startkey": 10, "descending": true});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(1, resp.rows[9].key);
  TEquals("1", resp.rows[9].id);

  testKeysSorted(resp, "rev");

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "descending": true});

  compareViewResults(resp, resp2);

  // test starkey query parameter with endkey and descending order
  resp = mergedQuery(dbs, "test/mapview1",
    {"startkey": 10, "endkey": 5, "descending": true});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(6, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);
  TEquals(5, resp.rows[5].key);
  TEquals("5", resp.rows[5].id);

  testKeysSorted(resp, "rev");

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"startkey": 10, "endkey": 5, "descending": true});

  compareViewResults(resp, resp2);


  // test key query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"key": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(1, resp.rows.length);
  TEquals(10, resp.rows[0].key);
  TEquals("10", resp.rows[0].id);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"key": 10});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"key": 1000});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(0, resp.rows.length);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"key": 1000});

  compareViewResults(resp, resp2);

  // test keys=[key1, key2, key3...] query parameter
  var keys = [5, 3, 10, 39, 666, 21];
  resp = mergedQuery(dbs, "test/mapview1", {"keys": keys});
  keys.sort(function(a, b) { return a - b; });

  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(keys.length - 1, resp.rows.length);

  for (i = 0; i < resp.rows.length; i++) {
    TEquals(keys[i], resp.rows[i].key);
  }

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"keys": keys});

  compareViewResults(resp, resp2);

  // test include_docs query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"include_docs": "true"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);

  testKeysSorted(resp);
  for (i = 0; i < resp.rows.length; i++) {
    var doc = resp.rows[i].doc;
    T((doc !== null) && (typeof doc === 'object'), "row has doc");
    TEquals(i + 1, doc.integer);
    TEquals(String(i + 1), doc.string);
    TEquals(resp.rows[i].id, doc._id);
  }

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/mapview1",
    {"include_docs": "true"});

  compareViewResults(resp, resp2);

  /**
   * End of tests with map views.
   */


  /**
   * Tests with reduce views
   */

  // compare query results with the results from a single full view
  var dbFull = newDb("test_db_full");
  docs = makeDocs(1, 91);
  populateAlternated([dbFull], docs);
  addDoc([dbFull], ddoc);

  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbC = newDb("test_db_c");
  dbs = [dbA, dbB, dbC];

  addDoc(dbs, ddoc);
  populateAlternated(dbs, docs);

  var respFull, respMerged;

  respFull = dbFull.view("test/redview1", {"reduce": false});
  respMerged = mergedQuery(dbs, "test/redview1", {"reduce": false});

  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"reduce": "false"});

  compareViewResults(respFull, respMerged);

  respFull = dbFull.view("test/redview1", {"group": false});
  respMerged = mergedQuery(dbs, "test/redview1", {"group": false});

  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"group": false});

  compareViewResults(respFull, respMerged);

  respFull = dbFull.view("test/redview1", {"group_level": 1});
  respMerged = mergedQuery(dbs, "test/redview1", {"group_level": 1});

  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"group_level": 1});

  compareViewResults(respFull, respMerged);

  respFull = dbFull.view("test/redview1", {"group": true});
  respMerged = mergedQuery(dbs, "test/redview1", {"group": true});

  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"group": true});

  compareViewResults(respFull, respMerged);

  var startkey = [9, "8"];
  var startkeyJson = JSON.stringify(startkey);

  respFull = dbFull.view("test/redview1",
    {"group": true, "startkey": startkey});
  respMerged = mergedQuery(dbs, "test/redview1",
    {"group": true, "startkey": startkeyJson});

  TEquals(startkeyJson, JSON.stringify(respFull.rows[0].key),
    "correct startkey with ?group=true");
  TEquals(startkeyJson, JSON.stringify(respMerged.rows[0].key),
    "correct startkey with ?group=true");
  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"group": true, "startkey": startkeyJson});

  TEquals(startkeyJson, JSON.stringify(respMerged.rows[0].key),
    "correct startkey with ?group=true");

  compareViewResults(respFull, respMerged);


  var endkey = [26, "25"];
  var endkeyJson = JSON.stringify(endkey);

  respFull = dbFull.view("test/redview1",
    {"group": true, "startkey": startkey, "endkey": endkey});
  respMerged = mergedQuery(dbs, "test/redview1",
    {"group": true, "startkey": startkeyJson, "endkey": endkeyJson});

  TEquals(startkeyJson, JSON.stringify(respFull.rows[0].key),
    "correct startkey with ?group=true");
  TEquals(startkeyJson, JSON.stringify(respMerged.rows[0].key),
    "correct startkey with ?group=true");

  i = respFull.rows.length - 1;
  TEquals(endkeyJson, JSON.stringify(respFull.rows[i].key),
    "correct endkey with ?group=true");
  i = respMerged.rows.length - 1;
  TEquals(endkeyJson, JSON.stringify(respMerged.rows[i].key),
    "correct endkey with ?group=true");

  compareViewResults(respFull, respMerged);

  // same, but with remote dbs
  respMerged = mergedQuery([dbUri(dbA), dbB, dbUri(dbC)], "test/redview1",
    {"group": true, "startkey": startkeyJson, "endkey": endkeyJson});

  TEquals(startkeyJson, JSON.stringify(respMerged.rows[0].key),
    "correct startkey with ?group=true");

  i = respMerged.rows.length - 1;
  TEquals(endkeyJson, JSON.stringify(respMerged.rows[i].key),
    "correct endkey with ?group=true");

  compareViewResults(respFull, respMerged);


  /**
   * End of tests with reduce views.
   */


  // cleanup
  dbA.deleteDb();
  dbB.deleteDb();
  dbC.deleteDb();
  dbD.deleteDb();
  dbE.deleteDb();
};
