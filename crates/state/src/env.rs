use rkv::{EnvironmentFlags, Manager, Rkv};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};

const DEFAULT_INITIAL_MAP_SIZE: usize = 100 * 1024 * 1024;
const MAX_DBS: u32 = 32;

/// Standard way to create an Rkv object representing an LMDB environment
pub fn create_lmdb_env(path: &Path) -> Arc<RwLock<Rkv>> {
    let initial_map_size = None;
    let flags = None;
    Manager::singleton()
        .write()
        .unwrap()
        .get_or_create(path, |path: &Path| {
            let mut env_builder = Rkv::environment_builder();
            env_builder
                // max size of memory map, can be changed later
                .set_map_size(initial_map_size.unwrap_or(DEFAULT_INITIAL_MAP_SIZE))
                // max number of DBs in this environment
                .set_max_dbs(MAX_DBS)
                // These flags make writes waaaaay faster by async writing to disk rather than blocking
                // There is some loss of data integrity guarantees that comes with this
                .set_flags(
                    flags.unwrap_or_else(|| {
                        EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC
                    }),
                );
            Rkv::from_env(path, env_builder)
        })
        .unwrap()
}

#[cfg(test)]
pub mod test {
    use crate::{error::WorkspaceResult, env::create_lmdb_env};
    use rkv::{Writer, Rkv};
    use std::sync::{Arc, RwLock};
    use tempdir::TempDir;

    pub fn test_env() -> Arc<RwLock<Rkv>> {
        let tmpdir = TempDir::new("skunkworx").unwrap();
        create_lmdb_env(tmpdir.path())
    }

    pub fn with_writer<F>(env: &Rkv, f: F)
    where F: FnOnce(&mut Writer) -> WorkspaceResult<()> {
        let mut writer = env.write().unwrap();
        let result = f(&mut writer).unwrap();
        writer.commit().unwrap();
    }
}