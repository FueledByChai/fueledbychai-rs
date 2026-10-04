//! A reader for Paradex's Simple Binary Encoding frames, gated on the block lengths each frame
//! states (design §15: a schema rollout must not break decoding).
//!
//! The layout is Paradex's published schema, `paradex_1_0.xml` in
//! [tradeparadex/paradex-py](https://github.com/tradeparadex/paradex-py/blob/b8248fb747e278d2167ac2f056b339a287d5ef30/paradex_py/api/sbe/paradex_1_0.xml)
//! (pinned at commit `b8248fb747e278d2167ac2f056b339a287d5ef30`, the one named by
//! docs.paradex.trade's "Binary Encoding (SBE)" page). Every frame starts with an 8-byte
//! little-endian header (`blockLength`, `templateId`, `schemaId`, `version`); the root block of
//! `blockLength` bytes follows, then repeating groups (each with a 4-byte `blockLength` and
//! `numInGroup` header), then variable-length strings (`varString8`: a `uint8` length and that
//! many UTF-8 bytes).
//!
//! The reader trusts no fixed layout: a field is read only when it lies inside the block the
//! frame states, so a field past a shorter block is absent ([`Block::i64_at`] gives `None`) and
//! bytes past the known fields of a longer block are skipped; group entries are read by their
//! own stated length and count. A frame of another schema id, or one shorter than the block it
//! declares, is refused.

use fbc_core::DecodeError;

/// The schema id this reader decodes; any other is refused.
pub const SCHEMA_ID: u16 = 1;

/// The schema version this adapter negotiates (`sbeSchemaVersion`). The reader itself decodes
/// any version of schema 1 by its stated block lengths.
pub const SCHEMA_VERSION: u16 = 1;

/// The length of the message header.
pub const HEADER_LEN: usize = 8;

/// The null sentinel of the schema's optional `int64` mantissas (`Price8NULL`, `Qty8NULL`).
pub const NULL_I64: i64 = i64::MIN;

/// A message header.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Header {
    /// The root block's length in bytes.
    pub block_length: u16,
    /// Which message the frame is.
    pub template_id: u16,
    /// The schema the frame is encoded under.
    pub schema_id: u16,
    /// The schema version the frame is encoded under.
    pub version: u16,
}

/// Fixed-length fields: a message's root block or one group entry.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Block<'a>(&'a [u8]);

impl<'a> Block<'a> {
    /// The bytes of the block.
    pub fn bytes(&self) -> &'a [u8] {
        self.0
    }

    /// The `int64` at `offset`, or `None` when the block ends before it.
    pub fn i64_at(&self, offset: usize) -> Option<i64> {
        let bytes = self.0.get(offset..offset.checked_add(8)?)?;
        Some(i64::from_le_bytes(bytes.try_into().ok()?))
    }

    /// The `uint8` at `offset`, or `None` when the block ends before it.
    pub fn u8_at(&self, offset: usize) -> Option<u8> {
        self.0.get(offset).copied()
    }
}

/// One frame, split into its header, root block and what follows the block.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Message<'a> {
    header: Header,
    block: Block<'a>,
    tail: &'a [u8],
}

impl<'a> Message<'a> {
    /// Splits `frame`; refuses a frame shorter than the header, of another schema id than
    /// [`SCHEMA_ID`], or shorter than the root block its header declares.
    pub fn parse(frame: &'a [u8]) -> Result<Message<'a>, DecodeError> {
        if frame.len() < HEADER_LEN {
            return Err(DecodeError::Malformed("SBE frame shorter than its header"));
        }
        let u16_at = |at: usize| u16::from_le_bytes([frame[at], frame[at + 1]]);
        let header = Header {
            block_length: u16_at(0),
            template_id: u16_at(2),
            schema_id: u16_at(4),
            version: u16_at(6),
        };
        if header.schema_id != SCHEMA_ID {
            return Err(DecodeError::Malformed("SBE schema id"));
        }
        let rest = &frame[HEADER_LEN..];
        let Some((block, tail)) = rest.split_at_checked(usize::from(header.block_length)) else {
            return Err(DecodeError::Malformed(
                "SBE frame shorter than its declared block",
            ));
        };
        Ok(Message {
            header,
            block: Block(block),
            tail,
        })
    }

    pub fn header(&self) -> Header {
        self.header
    }

    /// The root block.
    pub fn block(&self) -> Block<'a> {
        self.block
    }

    /// A cursor over what follows the root block: groups, then variable-length data.
    pub fn tail(&self) -> Tail<'a> {
        Tail(self.tail)
    }
}

/// A cursor over the groups and variable-length data after a root block, read in schema order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Tail<'a>(&'a [u8]);

impl<'a> Tail<'a> {
    /// The next repeating group: its header, then `numInGroup` entries of its stated length.
    /// Refused when the frame ends before the group does.
    pub fn group(&mut self) -> Result<Group<'a>, DecodeError> {
        let short = DecodeError::Malformed("SBE group runs past the frame");
        let (dims, rest) = self.0.split_at_checked(4).ok_or(short)?;
        let entry_len = usize::from(u16::from_le_bytes([dims[0], dims[1]]));
        let count = usize::from(u16::from_le_bytes([dims[2], dims[3]]));
        // Both are u16, so the product fits a usize.
        let (entries, rest) = rest.split_at_checked(entry_len * count).ok_or(short)?;
        self.0 = rest;
        Ok(Group {
            entry_len,
            count,
            entries,
        })
    }

    /// The next `varString8`, or `None` when no bytes are left: data a later version appends
    /// is absent from an earlier version's frame. Refused when its bytes run past the frame or
    /// are not UTF-8.
    pub fn var_str(&mut self) -> Result<Option<&'a str>, DecodeError> {
        let Some((&len, rest)) = self.0.split_first() else {
            return Ok(None);
        };
        let (text, rest) = rest
            .split_at_checked(usize::from(len))
            .ok_or(DecodeError::Malformed("SBE string runs past the frame"))?;
        let text =
            core::str::from_utf8(text).map_err(|_| DecodeError::Malformed("SBE string UTF-8"))?;
        self.0 = rest;
        Ok(Some(text))
    }
}

/// A repeating group's entries, each read as a [`Block`] of the group's own entry length.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Group<'a> {
    entry_len: usize,
    count: usize,
    entries: &'a [u8],
}

impl<'a> Group<'a> {
    /// The number of entries.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entries, in order.
    pub fn entries(&self) -> impl Iterator<Item = Block<'a>> + 'a {
        let (entries, len) = (self.entries, self.entry_len);
        (0..self.count).map(move |i| Block(&entries[i * len..(i + 1) * len]))
    }
}
