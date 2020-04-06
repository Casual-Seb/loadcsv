const fs = require("fs");
const { Transform } = require("stream");
const _ = require("lodash");
const shuffleSeed = require("shuffle-seed");

function readStream(
  filename,
  {
    converters = {},
    dataColumns = [],
    labelColumns = [],
    shuffle = true,
    seed = "standard",
    splitTest = false,
  }
) {
  return new Promise((resolve, reject) => {
    let data = [];
    stream = fs
      .createReadStream(filename, { encoding: "UTF-8" })
      .pipe(clean(converters));
    stream.on("data", (chunk) => {
      data.push(chunk);
    });

    const fix = () => {
      data = data.flat(1);
      let labels = extractColumns(data, labelColumns);
      data = extractColumns(data, dataColumns);

      data.shift();
      labels.shift();

      if (shuffle) {
        data = shuffleSeed.shuffle(data, seed);
        labels = shuffleSeed.shuffle(labels, seed);
      }

      if (splitTest) {
        const trainSize = _.isNumber(splitTest)
          ? splitTest
          : Math.floor(data.length / 2);

        return {
          features: data.slice(0, trainSize),
          labels: labels.slice(0, trainSize),
          testFeatures: data.slice(trainSize),
          testLabels: labels.slice(trainSize),
        };
      } else {
        return { features: data, labels };
      }
    };

    stream.on("end", () => resolve(fix()));
    stream.on("error", (error) => reject(error));
  });
}

function clean(converters) {
  return new Transform({
    objectMode: true,
    transform(row, encoding, callback) {
      row = row.split("\n").map((l) => l.split(","));
      row = row.map((l) => _.dropRightWhile(l, (val) => val === ""));

      const headers = _.first(row);

      row = row.map((l, index) => {
        if (index === 0) {
          return l;
        }

        return l.map((element, index) => {
          if (converters[headers[index]]) {
            const converted = converters[headers[index]](element);
            return _.isNaN(converted) ? element : converted;
          }

          const result = parseFloat(element);
          return _.isNaN(result) ? element : result;
        });
      });

      callback(null, row);
    },
  });
}

function extractColumns(data, columnNames) {
  const headers = _.first(data);

  const indexes = _.map(columnNames, (column) => headers.indexOf(column));
  const extracted = _.map(data, (row) => _.pullAt(row, indexes));

  return extracted;
}

async function loadCSV(filename, options) {
  console.log("calling");
  const result = await readStream(filename, options);
  const { features, labels, testFeatures, testLabels } = result;

  console.log("Features:", features);
  console.log("Labels:", labels);
  console.log("TestFeatures:", testFeatures);
  console.log("TestLabels:", testLabels);
}

loadCSV("data.csv", {
  dataColumns: ["height", "value"],
  labelColumns: ["passed"],
  seed: "sdfsdf",
  shuffle: true,
  splitTest: 2,
  converters: {
    passed: (val) => (val === "TRUE" ? 1 : 0),
  },
});
