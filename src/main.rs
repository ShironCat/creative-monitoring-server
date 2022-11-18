use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, TryFutureExt};
use parking_lot::Once;
use serde::Deserialize;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        RwLock,
    },
    time::interval,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{
    any,
    reply::html,
    ws::{Message, WebSocket},
    Filter,
};

static ID: AtomicUsize = AtomicUsize::new(1);

type Sensors = Arc<RwLock<HashMap<usize, (String, UnboundedSender<Message>)>>>;

type Clients = Arc<RwLock<HashMap<usize, UnboundedSender<Message>>>>;

type Observer = Arc<RwLock<Once>>;

#[derive(Deserialize)]
struct SensorMessage {
    name: String,
}

#[derive(Deserialize)]
struct ClientMessage {
    target: String,
    content: String,
}

#[tokio::main]
async fn main() {
    let sensors = Sensors::default();
    let sensors = any().map(move || sensors.clone());

    let clients = Clients::default();
    let clients = any().map(move || clients.clone());

    let observer = Observer::default();
    let observer = any().map(move || observer.clone());

    let sensor = warp::path("sensor")
        .and(warp::ws())
        .and(sensors.clone())
        .and(clients.clone())
        .map(|ws: warp::ws::Ws, sensors, clients| {
            ws.on_upgrade(move |socket| sensor_connected(socket, sensors, clients))
        });

    let client = warp::path("client")
        .and(warp::ws())
        .and(clients)
        .and(sensors)
        .and(observer)
        .map(|ws: warp::ws::Ws, clients, sensors, observer| {
            ws.on_upgrade(move |socket| client_connected(socket, clients, sensors, observer))
        });

    let index = warp::path::end().map(|| html("OK"));

    let routes = index.or(sensor).or(client);

    warp::serve(routes)
        .run((
            [0, 0, 0, 0],
            std::env::args()
                .nth(1)
                .unwrap_or("8080".to_string())
                .parse::<u16>()
                .unwrap_or(8080),
        ))
        .await;
}

async fn sensor_connected(ws: WebSocket, sensors: Sensors, clients: Clients) {
    let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (mut sensor_ws_tx, mut sensor_ws_rx) = ws.split();
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);

    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            sensor_ws_tx.send(message).unwrap_or_else(|_| {}).await;
        }
    });

    sensors.write().await.insert(id, (String::new(), tx));

    while let Some(result) = sensor_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(_) => {
                break;
            }
        };
        sensor_message(id, msg, &clients, &sensors).await;
    }

    sensor_disconnected(id, &sensors).await;
}

async fn client_connected(ws: WebSocket, clients: Clients, sensors: Sensors, observer: Observer) {
    let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (mut client_ws_tx, mut client_ws_rx) = ws.split();
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);

    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            client_ws_tx.send(message).unwrap_or_else(|_| {}).await;
        }
    });

    clients.write().await.insert(id, tx);

    check_observer(observer, &clients, sensors.clone()).await;

    while let Some(result) = client_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(_) => {
                break;
            }
        };
        client_message(msg, &sensors).await;
    }

    client_disconnected(id, &clients).await;
}

async fn check_observer(observer: Arc<RwLock<Once>>, clients: &Clients, sensors: Sensors) {
    observer.clone().read().await.call_once(|| {
        let clients_clone = clients.clone();

        let mut interval = interval(Duration::from_secs(2));

        tokio::task::spawn(async move {
            while !clients_clone.read().await.is_empty() {
                sensors
                    .read()
                    .await
                    .iter()
                    .for_each(|(_, (_, tx))| tx.send(Message::text("poll")).unwrap_or(()));
                interval.tick().await;
            }

            *observer.write().await = Once::new();
        });
    });
}

async fn sensor_message(id: usize, msg: Message, clients: &Clients, sensors: &Sensors) {
    let msg = if let Ok(s) = msg.to_str() {
        s
    } else {
        return;
    };

    let sensor_message: SensorMessage = if let Ok(sm) = serde_json::from_str(msg) {
        sm
    } else {
        return;
    };

    clients
        .read()
        .await
        .iter()
        .for_each(|(_, tx)| tx.send(Message::text(msg)).unwrap_or(()));

    sensors
        .write()
        .await
        .entry(id)
        .and_modify(|(name, _)| *name = sensor_message.name);
}

async fn client_message(msg: Message, sensors: &Sensors) {
    let msg = if let Ok(s) = msg.to_str() {
        s
    } else {
        return;
    };

    let client_message: ClientMessage = if let Ok(cm) = serde_json::from_str(msg) {
        cm
    } else {
        return;
    };

    if let Some((_, (_, tx))) = sensors
        .read()
        .await
        .iter()
        .find(|(_, (name, _))| **name == client_message.target)
    {
        tx.send(Message::text(client_message.content)).unwrap_or(())
    };
}

async fn sensor_disconnected(id: usize, sensors: &Sensors) {
    sensors.write().await.remove(&id);
}

async fn client_disconnected(id: usize, clients: &Clients) {
    clients.write().await.remove(&id);
}
