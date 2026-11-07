//! Walking the chain of batched roots.
use std::pin::Pin;
use std::sync::Arc;

use hypercore::Session;

use crate::context::Context;
use crate::encoding::Block;
use crate::{Bee, Error, Head, Result};
type DownloadWindow =
    Pin<Box<dyn Future<Output = std::result::Result<bool, hypercore::CoreError>> + Send>>;

/// One historical batch, with decoded blocks in append order.
pub struct Change {
    pub head: Head,
    pub tail: Option<Head>,
    pub batch: Vec<Block>,
}

/// A backwards stream of changes. Dropping it cancels its prefetch window.
pub struct ChangesStream {
    head: Option<Head>,
    context: Option<Arc<Context>>,
    prefetch: u64,
    window: Option<DownloadWindow>,
    window_core: Option<Arc<Session>>,
    window_start: u64,
    read_options: crate::ReadOptions,
}

impl ChangesStream {
    pub(crate) async fn new(bee: Bee, head: Option<Head>, prefetch: u64) -> Result<Self> {
        let head = head.or_else(|| bee.head());
        let context = match head {
            Some(head) => Some(bee.context_by_key(head.key).await?),
            None => None,
        };
        Ok(Self {
            head,
            context,
            prefetch,
            window: None,
            window_core: None,
            window_start: 0,
            read_options: crate::ReadOptions::default(),
        })
    }

    pub(crate) fn with_read_options(mut self, options: crate::ReadOptions) -> Self {
        self.read_options = options;
        self
    }

    pub async fn next(&mut self) -> Result<Option<Change>> {
        let options = self.read_options.clone();
        crate::READ_OPTIONS.scope(options, self.next_inner()).await
    }

    async fn next_inner(&mut self) -> Result<Option<Change>> {
        let Some(head) = self.head.filter(|h| h.length != 0) else {
            self.window = None;
            self.window_core = None;
            return Ok(None);
        };
        let context = self
            .context
            .as_ref()
            .ok_or(Error::Message("missing changes context"))?
            .clone();
        let seq = head.length - 1;
        if self.prefetch > 0
            && (self
                .window_core
                .as_ref()
                .is_none_or(|c| c.key() != context.core.key())
                || seq < self.window_start)
        {
            self.window = None;
            self.window_start = head.length.saturating_sub(self.prefetch);
            self.window = Some(match self.read_options.bag.clone() {
                Some(bag) => Box::pin(context.core.download_with_bag(
                    self.window_start,
                    Some(head.length),
                    false,
                    bag,
                )),
                None => Box::pin(context.core.download(
                    self.window_start,
                    Some(head.length),
                    false,
                )),
            });
            self.window_core = Some(context.core.clone());
        }
        let last = context.get_block(seq, 0, None).await?;
        let start = seq
            .checked_sub(last.batch.start)
            .ok_or(Error::Message("invalid batch start"))?;
        let mut batch = Vec::new();
        for index in start..=seq {
            batch.push(if index == seq {
                last.clone()
            } else {
                context.get_block(index, 0, None).await?
            });
        }
        let tail = if let Some(previous) = last.previous {
            context.update_maybe(previous.core, Some(&last)).await?;
            let next = context.get_loaded_context(previous.core).await?;
            let tail = Head {
                key: *next.core.key(),
                length: previous.seq + 1,
            };
            self.context = Some(next);
            Some(tail)
        } else {
            self.context = None;
            self.window = None;
            self.window_core = None;
            None
        };
        self.head = tail;
        Ok(Some(Change { head, tail, batch }))
    }
}
