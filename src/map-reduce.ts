import * as childProcess from "child_process";
import * as path from "path";

export interface MapReduceFile {
    filename: string;
    words: Array<string>;
}

export interface MapReduceTuple {
    word: string;
    count: number;
}

/**
 * Generate random integer in range (min, max) inclusive
 * @param {number} min Minimum integer
 * @param {number} max Maximum interger
 * @returns {number} integer
 */
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
 * @param {boolean} parallelGroup If true, parallelize group stage
 * @param {Array<string>} filenames Array of filenames to perform word count on
 * @param {Function} callback Callback function to be invoked on completion
 * @returns {Map<string, number>} Map of (word, total count) pairs
 */
export function multiCoreMapReduce(numCores: number, parallelGroup: boolean, filenames: Array<string>, callback: Function) {
    const tick = Date.now();
    let childrenDone = 0;
    let all = new Array<MapReduceTuple>();
    // Assign files to child processes
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
    // Fork children
    const offset = 0;
    for (let i = 0; i < numCores; i++) {
        const filesToProcess = new Array<string>();
        for (let j = 0; j < filesPerChildren[i]; j++) {
            const name = filenames.pop();
            filesToProcess.push(name);
        }
        // Map Phase
        const child = childProcess.fork(path.resolve(__dirname, "child-map"));
        child.send(filesToProcess);
        child.on("message", (data: Array<MapReduceTuple>) => {
            console.log(`Child sent ${data.length} tuples`);
            childrenDone++;
            all = all.concat(data);
            child.kill("SIGINT");
            if (childrenDone === numCores) {
                console.log("All tuples returned from child processes");
                // Group Phase
                // Parallelize if Necessary
                if (parallelGroup) {
                    // Hold map tuples
                    const mapGroups = new Array<Array<[string, Array<number>]>>();
                    // Merge to map of counts
                    const mergedMap = new Map<string, Array<number>>();
                    // Assign pieces to children
                    const divPoint = Math.floor((all.length / numCores));
                    let childrenDone = 0;

                     // Parallel Group
                    for (let i = 0; i < numCores; i++) {
                        const groupChild = childProcess.fork(path.resolve(__dirname, "child-group"));
                        (i === numCores) ? groupChild.send(all.slice(i * divPoint, all.length)) : groupChild.send(all.slice((i * divPoint), ((i + 1) * divPoint))) ;
                        groupChild.on("message", (data: Array<[string, Array<number>]>) => {
                            childrenDone++;
                            mapGroups.push(data);
                            groupChild.kill("SIGINT");
                            if (childrenDone === numCores) {
                                console.log("All combined tuples returned from child processes");
                                mapGroups.forEach((mapGroup) => {
                                    mapGroup.forEach((tuple) => {
                                        const existing = mergedMap.get(tuple["0"]);
                                        if (existing && existing instanceof Array) {
                                            mergedMap.set(tuple["0"], existing.concat(tuple["1"]));
                                        }
                                        else {
                                            mergedMap.set(tuple["0"], Array.from(tuple["1"]));
                                        }
                                    });
                                });
                                // Reduce Phase
                                const finalCount = new Map<string, number>();
                                [...mergedMap.keys()].map((word: string) => {
                                    finalCount.set(word, reduce(word, mergedMap.get(word)));
                                });
                                callback(finalCount, tick);
                            }
                        });
                    }
                }
                else {
                    // Non-Parallelized Group
                    const tupleGroups = new Map<string, Array<number>>();
                    all.forEach((tuple: MapReduceTuple) => {
                        tupleGroups.has(tuple.word) ? tupleGroups.set(tuple.word, tupleGroups.get(tuple.word).concat(tuple.count)) : tupleGroups.set(tuple.word, new Array<number>(tuple.count));
                    });
                    // Reduce Phase
                    const finalCount = new Map<string, number>();
                    [...tupleGroups.keys()].map((word: string) => {
                        finalCount.set(word, reduce(word, tupleGroups.get(word)));
                    });
                    callback(finalCount, tick);
                }
            }
        });
    }
}
