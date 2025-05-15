use pgrx::{is_a, prelude::*};

/// All hooks needed to intercept and process CREATE DATABASE queries.
struct Hooks;
impl pgrx::PgHooks for Hooks {
    /// hook into the ProcessUtility hook to intercept CREATE DATABASE calls
    fn process_utility_hook(
        &mut self,
        pstmt: PgBox<pg_sys::PlannedStmt>,
        query_string: &core::ffi::CStr,
        read_only_tree: Option<bool>,
        context: pg_sys::ProcessUtilityContext,
        params: PgBox<pg_sys::ParamListInfoData>,
        query_env: PgBox<pg_sys::QueryEnvironment>,
        dest: PgBox<pg_sys::DestReceiver>,
        completion_tag: *mut pg_sys::QueryCompletion,
        prev_hook: fn(
            pstmt: PgBox<pg_sys::PlannedStmt>,
            query_string: &core::ffi::CStr,
            read_only_tree: Option<bool>,
            context: pg_sys::ProcessUtilityContext,
            params: PgBox<pg_sys::ParamListInfoData>,
            query_env: PgBox<pg_sys::QueryEnvironment>,
            dest: PgBox<pg_sys::DestReceiver>,
            completion_tag: *mut pg_sys::QueryCompletion,
        ) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        // only block CREATE DATABASE, forwarding all others
        // FIXME: check CREATEDB privilege of the user
        if unsafe { is_a(pstmt.utilityStmt, pg_sys::NodeTag_T_CreatedbStmt) } {
            let createdb =
                unsafe { PgBox::from_pg(pstmt.utilityStmt as *mut pg_sys::CreatedbStmt) };

            // parse and handle relevant options
            let mut strategy = None;
            let mut template = None;

            if !createdb.options.is_null() {
                let options = unsafe { PgBox::from_pg(createdb.options) };
                for index in 0..options.length {
                    let list_cell = unsafe { pg_sys::pgrx_list_nth(options.as_ptr(), index) };
                    let element = unsafe { PgBox::from_pg(list_cell as *mut pg_sys::DefElem) };
                    let defname = unsafe { core::ffi::CStr::from_ptr(element.defname) }
                        .to_str()
                        .expect("Invalid defname in CREATE DATABASE");

                    let arg = unsafe { PgBox::from_pg(element.arg) }
                        .to_string()
                        .replace('\"', "");

                    match defname {
                        "template" => {
                            template = Some(arg);
                        }
                        "strategy" => {
                            strategy = Some(arg.to_lowercase());
                        }
                        _ => (),
                    }
                }
            }

            // tylko jeśli STRATEGY == snapshot → wykonaj snapshot
            if matches!(strategy.as_deref(), Some("snapshot")) {
                let target = unsafe { core::ffi::CStr::from_ptr(createdb.dbname) }
                    .to_str()
                    .expect("Invalid dbname in CREATE DATABASE");

                crate::branch(target, template.as_deref());
                pgrx::HookResult::new(())
            } else {
                // wszystko inne → przekieruj do poprzedniego hooka
                prev_hook(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    completion_tag,
                )
            }
        } else {
            prev_hook(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            )
        }
    }
}

static mut HOOKS: Hooks = Hooks;

/// initialize all of the hooks for use with _PG_init
pub unsafe fn init() {
    pgrx::register_hook(&mut HOOKS)
}
