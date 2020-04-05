const fs = require("fs");
const { Transform } = require("stream");
const _ = require("lodash");

function readStream(filename, { converters = {} }) {
  return new Promise((resolve, reject) => {
    let data = [];
    stream = fs
      .createReadStream(filename, { encoding: "UTF-8" })
      .pipe(clean(converters));
    stream.on("data", (chunk) => {
      data.push(chunk);
    });
    stream.on("end", () => resolve(data.flat(1)));
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

async function loadCSV(filename, options) {
  console.log("calling");
  const result = await readStream(filename, options);
  console.log(result);
}

loadCSV("data.csv", {
  converters: {
    passed: (val) => (val === "TRUE" ? 1 : 0),
  },
});
