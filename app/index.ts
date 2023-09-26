import "dotenv/config";
import { createClient } from "redis";
import { createConnection, Connection } from "mysql2/promise";

const tableCreate = `create table mj_task( id varchar(32) not null primary key, properties json null, action varchar(20) null, status varchar(20) null, prompt varchar (3000) null, prompt_en varchar (3000) null, description text null, submit_time bigint null, start_time bigint null, finish_time bigint null, progress varchar(50) null, image_url varchar (3000) null, fail_reason varchar (1000) null, state varchar(500) null, buttons json null);`;

interface MjTask {
  id: string;
  properties: any | null;
  action: string | null;
  status: string | null;
  prompt: string | null;
  promptEn: string | null;
  description: string | null;
  submitTime: number | null;
  startTime: number | null;
  finishTime: number | null;
  progress: string | null;
  imageUrl: string | null;
  failReason: string | null;
  state: string | null;
  buttons: any | null;
}
const r2m = (taskStr: string) => {
  const task = JSON.parse(taskStr) as MjTask;
  return [
    task.id,
    task.properties ? JSON.stringify(task.properties) : null,
    task.action,
    task.status,
    task.prompt,
    task.promptEn,
    task.description,
    task.submitTime,
    task.startTime,
    task.finishTime,
    task.progress,
    task.imageUrl,
    task.failReason,
    task.state,
    task.buttons ? JSON.stringify(task.buttons) : null,
  ];
};

const createHandler = (
  redis: ReturnType<typeof createClient>,
  mysql: Connection
) => {
  return async (keys: string[]) => {
    if (keys.length === 0) {
      return;
    }
    try {
      const values = await redis.mGet(keys);
      const rows = values
        .filter((v): v is string => {
          if (typeof v !== "string") {
            return false;
          }
          try {
            JSON.parse(v) as MjTask;
            return true;
          } catch {
            console.log("parse failed, " + v);
            return false;
          }
        })
        .map(r2m);
      const sql =
        "INSERT IGNORE INTO mj_task (id, properties,action,status,prompt,prompt_en,description,submit_time,start_time,finish_time,progress,image_url,fail_reason,state,buttons) values ?;";
      const result = await mysql.query(sql, [rows]);
      console.log(
        `attempt to insert ${rows.length} rows, affected ${
          (result[0] as any).affectedRows
        } rows`
      );
    } catch (error) {
      console.log("error occurred, ", error);
    }
  };
};

async function main() {
  console.log(process.env.MYSQL_CONNECT_URL);
  console.log(process.env.REDIS_CONNECT_URL);
  let redis: ReturnType<typeof createClient> | null = null;
  let mysql: Connection | null = null;
  try {
    redis = createClient({
      url: process.env.REDIS_CONNECT_URL,
    });
    await redis.connect();
    mysql = await createConnection({
      uri: process.env.MYSQL_CONNECT_URL!,
      multipleStatements: true,
    });
    const handle = createHandler(redis, mysql);
    let cursor = 0;
    do {
      console.log("current cursor 1", cursor);
      const result = await redis.scan(cursor, {
        MATCH: "mj-task-store::*",
        COUNT: 300,
      });
      cursor = result.cursor;
      console.log("current cursor 2", cursor);
      await handle(result.keys);
    } while (cursor !== 0);
  } catch (error) {
    console.log("error occurred, ", error);
  } finally {
    redis!.quit();
    mysql!.end();
    console.log("finished!");
  }
}

main().then();
