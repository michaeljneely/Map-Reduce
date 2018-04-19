import * as childProcess from "child_process";
import * as path from "path";

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export interface MapReduceFile {
    filename: string;
    words: Array<string>;
}

export interface MapReduceTuple {
    word: string;
    count: number;
}

function randomIntInInterval(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1) + min);
}

/**
 * Reduce stage - Combine individual counts for a single word
 * @param {string} word Given word to accumulate counts of
 * @param {Array<number>} counts Individual counts
 * @returns {number} Total count for the given word
 */
const reduce = function(word: string, counts: Array<number>): number {
    return counts.reduce((total, count) => total + count, 0);
};

/**
 * Multi-Core Map Reduce Implementation to count occurrences of words in a list of files
 * @param {number} cores Number of CPU cores to use
 * @param {Array<string>} filenames Array of filenames to perform word count on
 * @param {Function} callback Callback function to be invoked on completion
 * @returns {Map<string, number>} Map of (word, total count) pairs
 */
export function multiCoreMapReduce(numCores: number, filenames: Array<string>, callback: Function) {
    const tick = Date.now();
    let childrenDone = 0;
    let all = new Array<MapReduceTuple>();
    const filesPerChildren = new Array<number>();
    let filesRemaining = filenames.length;
    let coresRemaining = numCores;
    const maxFilesPerCore = Math.round(filenames.length / numCores);
    while (filesRemaining > 0) {
        const a = randomIntInInterval(Math.floor(filesRemaining / coresRemaining), maxFilesPerCore);
        coresRemaining --;
        filesRemaining -= a;
        filesPerChildren.push(a);
    }
    console.log(`${filesPerChildren} files per children.`);
    const offset = 0;
    for (let i = 0; i < numCores; i++) {
        const filesToProcess = new Array<string>();
        for (let j = 0; j < filesPerChildren[i]; j++) {
            const name = filenames.pop();
            filesToProcess.push(name);
        }
        const child = childProcess.fork(path.resolve(__dirname, "map-reduce-child"));
        child.send(filesToProcess);
         child.on("message", (data: Array<MapReduceTuple>) => {
             console.log(`Child sent ${data.length} tuples`);
             childrenDone++;
             all = all.concat(data);
             if (childrenDone === numCores) {
                const finalCounts = new Map<string, number>();
                // Group Phase
                const groups = new Map<string, Array<number>>();
                all.forEach((tuple: MapReduceTuple) => {
                    groups.has(tuple.word) ? groups.set(tuple.word, groups.get(tuple.word).concat(tuple.count)) : groups.set(tuple.word, new Array<number>(tuple.count));
                });
                [...groups.keys()].map((word: string) => {
                    finalCounts.set(word, reduce(word, groups.get(word)));
                });
                callback(finalCounts, tick);
             }
         });
    }
}
