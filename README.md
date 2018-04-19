# Node.js Multi-core Map-Reduce

Counts the number of words in a list of files given.

Parameters:
- NumCores: Number of cores to use (max 4 on most machines)
- Files: Array of paths to files relative to this folder
- Callback: Callback to invoke when final Map is returned

Returns: Map<string, number> - (Word, Count) pairs

## Example
Shown in `main.ts`