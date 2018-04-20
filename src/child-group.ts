import { MapReduceTuple } from "./map-reduce";

process.on("message", (tuples: Array<MapReduceTuple>) => {
    console.log(`Group child process called with ${tuples.length} tuples.`);
    const groupedMap = new Map<string, Array<number>>();
    tuples.forEach((tuple: MapReduceTuple) => {
        groupedMap.has(tuple.word) ? groupedMap.set(tuple.word, groupedMap.get(tuple.word).concat(tuple.count)) : groupedMap.set(tuple.word, new Array<number>().concat(tuple.count));
    });
    process.send(Array.from(groupedMap));
});

process.on("SIGINT", () => {
    console.log(`Group child process killed`);
});
