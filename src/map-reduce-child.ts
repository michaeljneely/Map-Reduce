import * as fs from "fs-extra";
import * as path from "path";
import { MapReduceFile, MapReduceTuple } from "./map-reduce";

process.on("message", async (filenames: Array<string>) => {
    console.log(`Child process called with ${filenames.length} files.`);
    let tuples = new Array<MapReduceTuple>();
    if (filenames) {
        for (const filename of filenames) {
            const data = await fs.readFile(path.resolve(__dirname, "../", filename), "utf8");
            tuples = tuples.concat(map(filename, data.replace(/[\W_]+/g, " ").split(" ")));
        }
        process.send(tuples);
    }
});


/**
 * Map stage - Count frequencies of words in a single files
 * @param {string} filename Filename
 * @param {Array<string>} words All words in file
 * @returns {Array<[string,number]} Array of [word, frequency] tuples
 */
function map(filename: string, words: Array<string>): Array<MapReduceTuple> {
    const list = new Array<[string, number]>();
    const frequencyMap = new Map<string, number>();
    words.forEach((word: string) => {
        frequencyMap.has(word) ? frequencyMap.set(word, frequencyMap.get(word) + 1) : frequencyMap.set(word, 1);
    });
    return [...frequencyMap.keys()].map((word: string) => {
        return {
            word,
            count: frequencyMap.get(word)
        };
    });
}
