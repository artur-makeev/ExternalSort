const fs = require('fs');
const readline = require('readline');

async function sortLines(inputFilePath, outputFilePath, maxMemoryUsage) {
	fs.existsSync(outputFilePath, function(exists) {
		if(exists) {
			fs.unlink(outputFilePath);
		}
	});
	
	const inputReadStream = fs.createReadStream(inputFilePath, {
		encoding: 'utf-8',
	});
	const rl = readline.createInterface({
		input: inputReadStream,
		crlfDelay: Infinity,
	});

	let currentChunk = [];
	let currentSize = 0;
	let currentChunkIndex = 0;
	let tempFileName;
	let tempFiles = [];

	for await (const line of rl) {
		if (currentChunk.length === 0) {
			tempFileName = `temp_${currentChunkIndex}.txt`;
			tempFiles.push(tempFileName);
		}

		const lineSize = Buffer.from(line).length;

		if (currentSize + lineSize <= maxMemoryUsage) {
			currentChunk.push(line);
			currentSize += lineSize;
		} else {
			currentChunk.push(line);
			currentChunk.sort();
			for await (const chunkLine of currentChunk) {
				fs.writeFileSync(tempFileName, `${chunkLine}\n`, { flag: 'a' });
			}

			currentChunkIndex++;
			currentSize = 0;
			currentChunk = [];
		}
	}

	if (currentChunk.length > 0) {
		currentChunk.sort();
		for await (const chunkLine of currentChunk) {
			fs.writeFileSync(tempFileName, `${chunkLine}\n`, { flag: 'a' });
		}
	}

	while (tempFiles.length > 1) {
		const tempFile1 = tempFiles.pop();
		const tempFile2 = tempFiles.pop();

		const mergedFileName = await mergeSorted(tempFile1, tempFile2);
		tempFiles.push(mergedFileName);
	}

	if (tempFiles.length === 1) {
		const inputReadStream = fs.createReadStream(tempFiles[0], {
			encoding: 'utf-8',
		});
		const rl = readline.createInterface({
			input: inputReadStream,
			crlfDelay: Infinity,
		});

		for await (const line of rl) {
			if (line === '') continue;

			fs.writeFileSync(outputFilePath, `${line}\n`, { flag: 'a' });
		}

		fs.unlinkSync(tempFiles[0]);
	}
}

async function mergeSorted(chunk1, chunk2, outputFileName) {
	const mergedFileName = outputFileName || `merged_${Date.now()}.txt`;

	const rl1 = readline.createInterface({
		input: fs.createReadStream(chunk1),
		crlfDelay: Infinity,
	});

	const rl2 = readline.createInterface({
		input: fs.createReadStream(chunk2),
		crlfDelay: Infinity,
	});

	const it1 = rl1[Symbol.asyncIterator]();
	const it2 = rl2[Symbol.asyncIterator]();

	let line1 = await it1.next();
	let line2 = await it2.next();

	while (true) {
		if (line2.value === undefined && line1.value === undefined) {
			break;
		}

		if (line1.value === undefined) {
			fs.writeFileSync(mergedFileName, `${line2.value}\n`, { flag: 'a' });
			line2 = await it2.next();
		} else if (line2.value === undefined) {
			fs.writeFileSync(mergedFileName, `${line1.value}\n`, { flag: 'a' });
			line1 = await it1.next();
		} else {
			if (line1.value < line2.value) {
				fs.writeFileSync(mergedFileName, `${line1.value}\n`, { flag: 'a' });
				line1 = await it1.next();
			} else {
				fs.writeFileSync(mergedFileName, `${line2.value}\n`, { flag: 'a' });
				line2 = await it2.next();
			}
		}
	}

	fs.unlinkSync(chunk1);
	fs.unlinkSync(chunk2);

	return mergedFileName;
}

function main() {
    const args = process.argv.slice(2);

    if (args.length !== 3) {
        console.log('Usage: node ExternalSort.js %INPUT_FILE_NAME% %OUTPUT_FILE_NAME% %MAX_MEMORY_USAGE%');
        return;
    }

	const inputFile = args[0];
	const outputFile = args[1];
	const maxMemoryUsage = args[2];

    try {
		sortLines(inputFile, outputFile, maxMemoryUsage)
    } catch (error) {
        console.error('ERROR:', error.message);
    }
}

main();

