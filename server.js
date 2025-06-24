const express = require('express');
const Influx = require('influx');
const cors = require('cors');
const mariadb = require('mariadb');
const ModbusRTU = require("modbus-serial");

const app = express();
const port = 3000;
const modbusClient = new ModbusRTU();
const SERIAL_PORT = "/dev/ttyS0";

app.use(cors());

// InfluxDB 연결 설정
const influx = new Influx.InfluxDB({
    host: 'localhost',
    port: 8086,
    protocol: 'http',
    database: 'get_sample',
    username: 'zaris_admin',
    password: '12345678'
});

// MariaDB 연결 풀 생성
const pool = mariadb.createPool({
    host: '127.0.0.1',
    user: 'root',
    password: '1234',
    database: 'get_sample',
    connectionLimit: 10,
    connectTimeout: 10000, // 10초
    acquireTimeout: 10000, // 10초
    timeout: 60000, // 60초
    bigIntAsNumber: true  // BigInt를 Number로 변환
});

// JSON 파싱을 위한 미들웨어
app.use(express.json());

// 기본 라우트
app.get('/', (req, res) => {
    res.json({ message: 'InfluxDB API Server is running' });
});

app.get('/api/test-connection', async (req, res) => {
    try {
        const receivedData = req.query.data;
        res.status(200).json({ 
            send: receivedData,
            message: 'Data received successfully' 
        });
    } catch (error) {
        console.error('Error testing connection:', error);
        res.status(500).json({ error: 'Failed to test connection to InfluxDB' });
    }
});

// 데이터 조회 API
app.get('/api/power/real-time', async (req, res) => {
    try {
        const query = `
            SELECT *
            FROM "power_data"
            ORDER BY time DESC
            LIMIT 1
        `;

        const results = await influx.query(query);
        res.json(results);
    } catch (error) {
        console.error('Error fetching data:', error);
        res.status(500).json({ error: 'Failed to fetch data from InfluxDB' });
    }
});

app.get('/api/pq-event', async (req,res) => {
    let conn;
    try {
        conn = await pool.getConnection();
        const rows = await conn.query(
            'SELECT * FROM pq_event ORDER BY event_time DESC'
        );
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching PQ events:', error);
        res.status(500).json({ error: 'Failed to fetch PQ events from database' });
    } finally {
        if (conn) conn.release();
    }
})

app.get('/api/power/chart', async (req,res) => {
    const type = req.query.type;
    const period = req.query.period;
    
    try {
        let query;
        let timeRange;
        let interval;

        // Set time range and interval based on period
        switch(period) {
            case '1h':
                timeRange = '1h';
                interval = '1m';
                break;
            case '6h':
                timeRange = '6h';
                interval = '10m';
                break;
            case '12h':
                timeRange = '12h';
                interval = '30m';
                break;
            case '24h':
                timeRange = '24h';
                interval = '1h';
                break;
            case '7d':
                timeRange = '7d';
                interval = '12h';
                break;
            case '30d':
                timeRange = '30d';
                interval = '24h';
                break;
            default:
                timeRange = '24h';
                interval = '30m';
        }

        // Set measurement and field based on type
        let measurement = 'power_data';
        switch(type) {
            case 'line_voltage':
                query = `
                    SELECT mean("line_voltage_ab") as line_voltage_ab,
                           mean("line_voltage_bc") as line_voltage_bc,
                           mean("line_voltage_ca") as line_voltage_ca
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'phase_current':
                query = `
                    SELECT mean("phase_current_a") as phase_current_a,
                           mean("phase_current_b") as phase_current_b,
                           mean("phase_current_c") as phase_current_c
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'power':
                query = `
                    SELECT mean("total_power") as total_power,
                           mean("reactive_power") as reactive_power,
                           mean("total_apparent_power") as total_apparent_power
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'power_factor':
                query = `
                    SELECT mean("power_factor") as power_factor
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'frequency':   
                query = `
                    SELECT mean("frequency") as frequency
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'voltage_thd':
                query = `
                    SELECT mean("voltage_THD_ab") as voltage_THD_ab,
                           mean("voltage_THD_bc") as voltage_THD_bc,
                           mean("voltage_THD_ca") as voltage_THD_ca
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'current_thd':
                query = `
                    SELECT mean("current_THD_a") as current_THD_a,
                           mean("current_THD_b") as current_THD_b,
                           mean("current_THD_c") as current_THD_c
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            case 'energy':
                query = `
                    SELECT mean("active_power_energy_a") as active_power_energy_a,
                           mean("active_power_energy_b") as active_power_energy_b,
                           mean("active_power_energy_c") as active_power_energy_c
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
                break;
            default:
                query = `
                    SELECT mean("total_power") as value
                    FROM "${measurement}"
                    WHERE time >= now() - ${timeRange}
                    GROUP BY time(${interval})
                    FILL(null)
                `;
        }
        console.log(query);
        const results = await influx.query(query);
        res.status(200).json(results);
    } catch (error) {
        console.error('Error fetching chart data:', error);
        res.status(500).json({ error: 'Failed to fetch chart data from InfluxDB' });
    }
});

// 연결 재시도 함수
async function getConnectionWithRetry(maxRetries = 3, delay = 1000) {
    let retries = 0;
    while (retries < maxRetries) {
        try {
            return await pool.getConnection();
        } catch (error) {
            retries++;
            if (retries === maxRetries) throw error;
            console.log(`Connection attempt ${retries} failed, retrying in ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

// 서버 시작
const server = app.listen(port, '0.0.0.0', async (error) => {
    if (error) {
        console.error('Error starting server:', error);
        process.exit(1);
    }
    console.log(`Server is running on port ${port}`);
    console.log(`Server is accessible from external connections`);
    
    // Start periodic Modbus communication every 10 seconds
    try {
        // Initial run
        await PeriodicModbusCommunication();
        console.log('Initial Modbus communication completed');
        
        // Set up interval for periodic communication
        setInterval(async () => {
            try {
                await PeriodicModbusCommunication();
                console.log('Periodic Modbus communication completed');
            } catch (error) {
                console.error('Error in periodic Modbus communication:', error);
            }
        }, 10000); // 10 seconds interval
        
        console.log('Periodic Modbus communication started successfully');
    } catch (error) {
        console.error('Failed to start periodic Modbus communication:', error);
    }
});



async function readModbusData(startAddress, registerCount) {
    try {
        const data_set = await modbusClient.readInputRegisters(startAddress, registerCount); 
        return data_set.data;
    } catch (err) {
        console.error('Error reading Modbus data:', err);
        return null;
    }
}

// 에러 핸들링
process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
});

process.on('unhandledRejection', (error) => {
    console.error('Unhandled Rejection:', error);
});

server.on('error', (error) => {
    console.error('Server error:', error);
    if (error.code === 'EADDRINUSE') {
        console.error(`Port ${port} is already in use`);
    }
});


async function getModbusInfo() {
    let conn;
    try {
        conn = await pool.getConnection();
        const rows = await conn.query('SELECT * FROM modbus_info');
        return rows;
    } catch (err) {
        console.error('Error getting Modbus :', err);
        return [];
    } finally {
        if (conn) conn.release();
    }
}

async function setModbusArray() {
    const setDataList = [];
    let shouldContinue = true;

    while(shouldContinue) {
        try {
            const modbusResult = await modbusClient.readHoldingRegisters(4100, 15);
            if(modbusResult.data.length === 0) {
                shouldContinue = false;
                console.log("Slave device is busy. Stopping data collection.");
                break;
            }
            
            // Process the data
            const reg0 = modbusResult.data[0];
            const eventType = (reg0 >> 5) & 0b111;
            const eventChannel = (reg0 >> 3) & 0b11;
            const recordStatus = reg0 & 0b111;

            const flash_start = convertToUnsignedLong(modbusResult.data[1], modbusResult.data[2]);
            const flash_end = convertToUnsignedLong(modbusResult.data[3], modbusResult.data[4]);

            const duration_raw = convertToUnsignedLong(modbusResult.data[5], modbusResult.data[6]);
            const duration = duration_raw / 1920 * 1000;

            const max_value = convertFormatData(modbusResult.data[7], modbusResult.data[8]);
            const min_value = convertFormatData(modbusResult.data[9], modbusResult.data[10]);
            
            // BCD 형식의 날짜/시간 데이터 변환
            const year = bcdToDecimal((modbusResult.data[11] >> 8) & 0xFF) + 2000;
            const month = bcdToDecimal(modbusResult.data[11] & 0xFF);
            const day = bcdToDecimal((modbusResult.data[12] >> 8) & 0xFF);
            const hour = bcdToDecimal(modbusResult.data[12] & 0xFF);
            const minute = bcdToDecimal((modbusResult.data[13] >> 8) & 0xFF);
            const second = bcdToDecimal(modbusResult.data[13] & 0xFF);

            const datetime = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')} ${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`;

            const milliseconds = modbusResult.data[14] & 0xFFFF; 

            const regiArray = [0x1400, flash_start]
            await modbusClient.writeRegisters(6014, regiArray);
            await new Promise(resolve => setTimeout(resolve, 100));

            const wave0_raw = await modbusClient.readHoldingRegisters(6016, 1);
            const wave0 = wave0_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave1_raw = await modbusClient.readHoldingRegisters(6022, 1);
            const wave1 = wave1_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave2_raw = await modbusClient.readHoldingRegisters(6028, 1);
            const wave2 = wave2_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave3_raw = await modbusClient.readHoldingRegisters(6034, 1);
            const wave3 = wave3_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave4_raw = await modbusClient.readHoldingRegisters(6040, 1);
            const wave4 = wave4_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave5_raw = await modbusClient.readHoldingRegisters(6046, 1);
            const wave5 = wave5_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave6_raw = await modbusClient.readHoldingRegisters(6052, 1);
            const wave6 = wave6_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave7_raw = await modbusClient.readHoldingRegisters(6058, 1);
            const wave7 = wave7_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave8_raw = await modbusClient.readHoldingRegisters(6064, 1);
            const wave8 = wave8_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave9_raw = await modbusClient.readHoldingRegisters(6070, 1);
            const wave9 = wave9_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave10_raw = await modbusClient.readHoldingRegisters(6076, 1);
            const wave10 = wave10_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave11_raw = await modbusClient.readHoldingRegisters(6082, 1);
            const wave11 = wave11_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave12_raw = await modbusClient.readHoldingRegisters(6088, 1);
            const wave12 = wave12_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave13_raw = await modbusClient.readHoldingRegisters(6094, 1);
            const wave13 = wave13_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave14_raw = await modbusClient.readHoldingRegisters(6100, 1);
            const wave14 = wave14_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave15_raw = await modbusClient.readHoldingRegisters(6106, 1);
            const wave15 = wave15_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave16_raw = await modbusClient.readHoldingRegisters(6112, 1);
            const wave16 = wave16_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave17_raw = await modbusClient.readHoldingRegisters(6118, 1);
            const wave17 = wave17_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave18_raw = await modbusClient.readHoldingRegisters(6124, 1);
            const wave18 = wave18_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            const wave19_raw = await modbusClient.readHoldingRegisters(6130, 1);
            const wave19 = wave19_raw.data[0];
            await new Promise(resolve => setTimeout(resolve, 50));
            
            
            
            

            // Store the processed data
            setDataList.push({
                flash_start,
                flash_end,
                eventType,
                eventChannel,
                recordStatus,
                duration,
                max_value,
                min_value,
                datetime,
                milliseconds,
                wave0,
                wave1,
                wave2,
                wave3,
                wave4,
                wave5,
                wave6,
                wave7,
                wave8,
                wave9,
                wave10,
                wave11,
                wave12,
                wave13,
                wave14,
                wave15,
                wave16,
                wave17,
                wave18,
                wave19
            });

            // Wait for 500ms before next read
            await new Promise(resolve => setTimeout(resolve, 500));

        } catch (readErr) {
            if (readErr.message.includes("exception 16")) {
                shouldContinue = false;
                console.log("Slave device is busy. Stopping data collection.");
            } else {
                throw readErr;
            }
        }
    }
    return setDataList;
}

// 32비트 Unsigned Long 변환 함수
function convertToUnsignedLong(highRegister, lowRegister) {
    return (highRegister << 16) | lowRegister;
}
  
function convertFormatData(registerHigh, registerLow) {
    const buffer = Buffer.alloc(4);
    buffer.writeUInt16BE(registerHigh, 0);
    buffer.writeUInt16BE(registerLow, 2);
    return buffer.readFloatBE(0);
}

// BCD 값을 10진수로 변환하는 함수
function bcdToDecimal(bcd) {
    return ((bcd >> 4) * 10) + (bcd & 0x0F);
}

// Convert flash_start to hex string
function convertToHex(flashStart) {
    return '0x' + flashStart.toString(16).toUpperCase().padStart(8, '0');
}

async function connectModbus(baudRate, slaveID) {
    try {
        // Ensure any existing connection is closed
        if (modbusClient.isOpen) {
            await modbusClient.close();
            console.log('Closed existing connection');
        }

        await modbusClient.connectRTUBuffered(SERIAL_PORT, { 
            baudRate: baudRate,
            dataBits: 8,
            stopBits: 1,
            parity: 'none'
        });
        modbusClient.setID(slaveID);
        console.log(`Modbus connection established with baud rate: ${baudRate}`);
        return true;
    } catch (err) {
        console.error('Error connecting to Modbus:', err);
        return false;
    }
}

// 10초마다 모드버스 통신을 수행하는 함수
async function PeriodicModbusCommunication() {    
    // Connect to Modbus device

    const modbusInfo = await getModbusInfo();
    if (modbusInfo.length === 0) {
        throw new Error('No active Modbus found');
    }
    
    try{
        const connected = await connectModbus(modbusInfo[0].baudRate, modbusInfo[0].slaveID);
        if(!connected) {
            throw new Error('Failed to connect to Modbus');
        }

        const data_set1 = await readModbusData(4, 34);
        await new Promise(resolve => setTimeout(resolve, 50));

        const data_set2 = await readModbusData(106, 12);
        await new Promise(resolve => setTimeout(resolve, 50));

        const data_set3 = await readModbusData(134, 18);
        await new Promise(resolve => setTimeout(resolve, 50));

        const data_set = [...data_set1, ...data_set2, ...data_set3];
        if(data_set.length !== 64) {
            throw new Error('Invalid data length');
        }
        await new Promise(resolve => setTimeout(resolve, 50));

        const sag_data = await setModbusArray();

        const power_data = setModbusData(data_set);
        if(power_data) {
            try {
                await influx.writePoints([
                    {
                        measurement: 'power_data',
                        tags: {
                            power: 'all'
                        },
                        fields: {
                            ...power_data
                        }
                    }
                ]); 
                console.log('Data successfully written to InfluxDB');
            } catch (err) {
                console.error('Error writing to InfluxDB:', err);
            }
        }

        if(sag_data.length > 0) {
            try {
                let conn;
                conn = await getConnectionWithRetry();
                for (const data of sag_data) {
                    try {
                        await conn.query(
                            'INSERT INTO pq_event (flash_start, flash_end, event_type, event_channel, event_status, duration, max_value, min_value, event_time, milliseconds, wave0, wave1, wave2, wave3, wave4, wave5, wave6, wave7, wave8, wave9, wave10, wave11, wave12, wave13, wave14, wave15, wave16, wave17, wave18, wave19) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            [data.flash_start, data.flash_end, data.eventType, data.eventChannel, data.recordStatus, data.duration, data.max_value, data.min_value, data.datetime, data.milliseconds, data.wave0, data.wave1, data.wave2, data.wave3, data.wave4, data.wave5, data.wave6, data.wave7, data.wave8, data.wave9, data.wave10, data.wave11, data.wave12, data.wave13, data.wave14, data.wave15, data.wave16, data.wave17, data.wave18, data.wave19]
                        );
                    } catch (queryError) {
                        console.error('Error inserting data:', queryError);
                        // 연결이 끊어진 경우 재연결 시도
                        if (queryError.name === 'TransactionTimedOutError') {
                            conn = await getConnectionWithRetry();
                            // 실패한 쿼리 재시도
                            await conn.query(
                                'INSERT INTO pq_event (flash_start, flash_end, event_type, event_channel, event_status, duration, max_value, min_value, event_time, milliseconds, wave0, wave1, wave2, wave3, wave4, wave5, wave6, wave7, wave8, wave9, wave10, wave11, wave12, wave13, wave14, wave15, wave16, wave17, wave18, wave19) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                [data.flash_start, data.flash_end, data.eventType, data.eventChannel, data.recordStatus, data.duration, data.max_value, data.min_value, data.datetime, data.milliseconds, data.wave0, data.wave1, data.wave2, data.wave3, data.wave4, data.wave5, data.wave6, data.wave7, data.wave8, data.wave9, data.wave10, data.wave11, data.wave12, data.wave13, data.wave14, data.wave15, data.wave16, data.wave17, data.wave18, data.wave19]
                            );
                        } else {
                            throw queryError;
                        }
                    }
                }
                console.log('Data successfully written to MariaDB');
            } catch (err) {
                console.error('Error writing to MariaDB:', err);
            }
        }   
    } catch (err) {
        console.error('Error in PeriodicModbusCommunication:', err);
    } finally {
        if (modbusClient.isOpen) {
            await modbusClient.close();
            console.log('Modbus connection closed');
        }
    }
}

function setModbusData(data_set) {
    if(data_set.length === 0) {
        return;
    }

    const power_data = {
        phase_current_a: convertFormatData(data_set[0], data_set[1]),
        phase_voltage_a: convertFormatData(data_set[6], data_set[7]),
        line_voltage_ab: convertFormatData(data_set[12], data_set[13]),
        active_power_energy_a: convertFormatData(data_set[34], data_set[35]),
        reactive_power_energy_a: convertFormatData(data_set[40], data_set[41]),
        current_THD_a: convertFormatData(data_set[52], data_set[53]),
        voltage_THD_ab: convertFormatData(data_set[58], data_set[59]),

        phase_current_b: convertFormatData(data_set[2], data_set[3]),
        phase_voltage_b: convertFormatData(data_set[8], data_set[9]),
        line_voltage_bc: convertFormatData(data_set[14], data_set[15]),
        active_power_energy_b: convertFormatData(data_set[36], data_set[37]),
        reactive_power_energy_b: convertFormatData(data_set[42], data_set[43]),
        current_THD_b: convertFormatData(data_set[54], data_set[55]),
        voltage_THD_bc: convertFormatData(data_set[60], data_set[61]),

        phase_current_c: convertFormatData(data_set[4], data_set[5]),
        phase_voltage_c: convertFormatData(data_set[10], data_set[11]),
        line_voltage_ca: convertFormatData(data_set[16], data_set[17]),
        active_power_energy_c: convertFormatData(data_set[38], data_set[39]),
        reactive_power_energy_c: convertFormatData(data_set[44], data_set[45]),
        current_THD_c: convertFormatData(data_set[56], data_set[57]),
        voltage_THD_ca: convertFormatData(data_set[62], data_set[63]),

        power_factor: convertFormatData(data_set[18], data_set[19]),
        total_power: convertFormatData(data_set[20], data_set[21]),
        total_reactive_power: convertFormatData(data_set[22], data_set[23]),
        total_apparent_power: convertFormatData(data_set[24], data_set[25]),
        frequency: convertFormatData(data_set[26], data_set[27]),
        active_power: convertFormatData(data_set[28], data_set[29]),
        reactive_power: convertFormatData(data_set[22], data_set[23]),
    }

    return power_data;
}

