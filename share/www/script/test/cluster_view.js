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

couchTests.cluster_view = function(debug) {
  if (debug) debugger;

  function newDb(name) {
    var db = new CouchDB(name, {"X-Couch-Full-Commit": "false"});
    db.deleteDb();
    db.createDb();

    return db;
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

  function clusterQuery(dbs, viewname) {
    var dbNames = [];

    for (var i = 0; i < dbs.length; i++) {
      dbNames.push(dbs[i].name);
    }

    var xhr = CouchDB.request("POST", "/_cluster_view", {
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        "databases": dbNames,
        "viewname": viewname
      })
    });
    TEquals(200, xhr.status);

    return JSON.parse(xhr.responseText);
  }

  function testKeysSorted(resp) {
    for (var i = 0; i < (resp.rows.length - 1); i++) {
      var row = resp.rows[i];
      var nextRow = resp.rows[i + 1];

      T(row.key < nextRow.key, "keys are sorted");
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
      }
    }
  };

  // test with empty dbs
  var dbA, dbB, dbs, docs, resp, i;
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);

  resp = clusterQuery(dbs, "test/mapview1");

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

  resp = clusterQuery(dbs, "test/mapview1");

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

  resp = clusterQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);


  // 2 dbs, sequenced keys (worst case)
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = [makeDocs(1, 21), makeDocs(21, 41)];
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateSequenced(dbs, docs);

  resp = clusterQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);


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

  resp = clusterQuery(dbs, "test/mapview1");

  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);

  testKeysSorted(resp);

  /**
   * End of tests with map views.
   */

  // cleanup
  dbA.deleteDb();
  dbB.deleteDb();
  dbC.deleteDb();
  dbD.deleteDb();
  dbE.deleteDb();
};
