#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::io;
use std::sync::Arc;
// use time::Duration;

use bytes::BytesMut;
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use may_postgres::{Client, Statement};
// use smallvec::SmallVec;
use yarte::{ywrite_html, Serialize};

#[derive(Serialize)]
pub struct Tes {
    nama: String
}

struct PgConnectionPool {
    clients: Vec<PgConnection>,
}

impl PgConnectionPool {
    fn new(db_url: &'static str, size: usize) -> PgConnectionPool {
        let clients = (0..size)
            .map(|_| may::go!(move || PgConnection::new(db_url)))
            .collect::<Vec<_>>();
        let mut clients: Vec<_> = clients.into_iter().map(|t| t.join().unwrap()).collect();
        clients.sort_by(|a, b| (a.client.id() % size).cmp(&(b.client.id() % size)));
        PgConnectionPool { clients }
    }

    fn get_connection(&self, id: usize) -> PgConnection {
        let len = self.clients.len();
        let connection = &self.clients[id % len];
        // assert_eq!(connection.client.id() % len, id % len);
        PgConnection {
            client: connection.client.clone(),
            statement: connection.statement.clone(),
        }
    }
}

struct PgStatement {
    tes: Statement,
    home: Statement,
}

struct PgConnection {
    client: Client,
    statement: Arc<PgStatement>,
}

impl PgConnection {
    fn new(db_url: &str) -> Self {
        let client = may_postgres::connect(db_url).unwrap();

        let tes = client.prepare("SELECT * FROM tes WHERE nama = $1").unwrap();
        let home = client.prepare("SELECT * FROM tes").unwrap();


        let statement = Arc::new(PgStatement {
            tes,
            home
        });

        PgConnection { client, statement }
    }


    fn tes_json(&self, nama: String) -> Result<Tes, may_postgres::Error> {
        let mut q = self
            .client
            .query_raw(&self.statement.tes, &[&nama])?;
        match q.next().transpose()? {
            Some(row) =>
            Ok(Tes {
                nama: row.get(0),
           }),
            None => unreachable!("nama={}", nama),
        }
    }


    fn home(&self, buf: &mut BytesMut) -> Result<(), may_postgres::Error> {
        let rows = self.client.query_raw(&self.statement.home, &[])?;

        let all_rows = Vec::from_iter(rows.map(|r| r.unwrap()));
        let mut fortunes = Vec::with_capacity(all_rows.len() + 1);
        fortunes.extend(all_rows.iter().map(|r| Tes {
            nama: r.get(0),
        }));

        let mut body = unsafe { std::ptr::read(buf) };
        ywrite_html!(body, "{{> index }}");
        unsafe { std::ptr::write(buf, body) };
        Ok(())
    }
}

struct ServerState {
    db: PgConnection
}

impl HttpService for ServerState {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/plaintext" => {
                rsp.header("Content-Type: text/plain").body("Hello, World!");
            }
            "/" => {
                rsp.header("Content-Type: text/html; charset=utf-8");
                self.db.home(rsp.body_mut()).unwrap();
            }
            p if p.starts_with("/api") => {
                rsp.header("Content-Type: application/json");
                let nama = String::from("fuji");
                let hasil = self.db.tes_json(nama).unwrap();
                hasil.to_bytes_mut(rsp.body_mut())
            }
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }

        Ok(())
    }
}

struct HttpServer {
    db_pool: PgConnectionPool,
}

impl HttpServiceFactory for HttpServer {
    type Service = ServerState;

    fn new_service(&self, id: usize) -> Self::Service {
        let db = self.db_pool.get_connection(id);
        ServerState { db }
    }
}

fn main() {
    may::config().set_pool_capacity(1000).set_stack_size(0x1000);
    println!("Starting http server: 0.0.0.0:8080");
    let server = HttpServer {
        db_pool: PgConnectionPool::new(
            "postgres://fuji:fuji@localhost/tes",
            num_cpus::get(),
        ),
    };
    server.start("0.0.0.0:8080").unwrap().join().unwrap();
}
