import { cpus } from "os";
import { multiCoreMapReduce } from "./src/map-reduce";

const print = (finalCounts: Map<string, number>, time: number) => {
    console.log(`Took: ${Date.now() - time} seconds`);
    process.exit(0);
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function main() {
    try {
        const maxCores = cpus().length;
        // process argv[0] is node
        // process argv[1] is filename
        const numCores = parseInt(process.argv[2]);
        if (numCores < 0 || numCores > maxCores) {
            throw new RangeError(`Number of Cores must be greater than 0 and less than or equal to ${numCores}.`);
        }
        multiCoreMapReduce(numCores, process.argv[3] === "true", process.argv.slice(4), print);
    }
    catch (error) {
        console.log(error);
        process.exit(1);
    }
}

main();
