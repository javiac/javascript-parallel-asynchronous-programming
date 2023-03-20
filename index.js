let nConnectionsFailed = 0;
let nDownloaded = 0;
let indexNextFile = 0;
let connectionPool = [];

const pooledDownload = async (connect, save, downloadList, maxConcurrency) => {
    /*return connect().then((connection) => {
        const { download, close } = connection
        return download(downloadList[0]).then((result) => save(result)) // download the first file and save the result
    })*/

    nConnectionsFailed = 0;
    nDownloaded = 0;
    indexNextFile = 0;
    connectionPool = [];

    await new Promise(async (resolve, reject) => {
        let i = 0;
        for (i = 0; i < maxConcurrency; i++) {
            try {
                const connection = await connect();
                connectionPool.push({ connection, nDownloads: 0 });

                (() => {
                    const index = i;
                    downloadWithConnection(connection, downloadList, resolve, reject, save, index)
                })()
            } catch (e) {
                break;
            }
        }

        if (connectionPool.length === 0) {
            reject(new Error('connection failed'));
        }
    })

    closeConnections();
}

async function downloadWithConnection(connection, downloadList, resolve, reject, save, index) {
    connectionPool[index].status = 'downloading';
    const file = downloadList[indexNextFile];

    if (!file) {
        return;
    }

    indexNextFile += 1;

    const { download } = connection

    return download(file)
        .then(async (result) => {
            connectionPool[index].status = 'done';
            connectionPool[index].nDownloads += 1;
            await save(result)
            nDownloaded += 1;
            if (nDownloaded === downloadList.length) {
                resolve();
            } else {
                let chosenConnectionIndex = -1;
                for (let i = 0; i < connectionPool.length; i++) {
                    if (connectionPool[i].status !== 'downloading' && (chosenConnectionIndex === -1 || connectionPool[i].nDownloads < connectionPool[chosenConnectionIndex].nDownloads)) {
                        chosenConnectionIndex = i;
                    }
                }

                downloadWithConnection(connectionPool[chosenConnectionIndex].connection, downloadList, resolve, reject, save, chosenConnectionIndex);
            }

        })
        .catch((error) => {
            closeConnections()
            reject(error);
        })
}

function closeConnections() {
    for (const connection of connectionPool) {
        connection.connection.close();
    }
}

module.exports = pooledDownload
