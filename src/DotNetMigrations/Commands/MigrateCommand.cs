using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using DotNetMigrations.Core;
using DotNetMigrations.Core.Data;
using DotNetMigrations.Migrations;
using DotNetMigrations.Repositories;

namespace DotNetMigrations.Commands
{
    public class MigrateCommand : DatabaseCommandBase<MigrateCommandArgs>
    {
        private readonly IMigrationDirectory _migrationDirectory;

        public MigrateCommand()
            : this(new MigrationDirectory())
        {
        }

        public MigrateCommand(IMigrationDirectory migrationDirectory)
        {
            _migrationDirectory = migrationDirectory;
        }

        /// <summary>
        /// The name of the command that is typed as a command line argument.
        /// </summary>
        public override string CommandName
        {
            get { return "migrate"; }
        }

        /// <summary>
        /// The help text information for the command.
        /// </summary>
        public override string Description
        {
            get { return "Migrates the database up or down to a specific version."; }
        }

        /// <summary>
        /// Executes the Command's logic.
        /// </summary>
        protected override void Execute(MigrateCommandArgs args)
        {
            var files = _migrationDirectory.GetScripts()
                .OrderByDescending(x => x.Version);

            var oneScopePerScript = args.TransactionScope == "script";

            if (!files.Any())
            {
                Log.WriteLine("No migration scripts were found.");
                return;
            }

            var currentVersion = GetDatabaseVersion();
            var targetVersion = args.TargetVersion;

            if (targetVersion == -1)
            {
                //  if version not provided, assume we want to migrate to the latest migration script version
                targetVersion = files.Select(x => x.Version).First();
            }

            Log.WriteLine("Database is at version:".PadRight(30) + currentVersion);

            MigrationDirection direction;
            if (currentVersion < targetVersion)
            {
                direction = MigrationDirection.Up;
                MigrateUp(currentVersion, targetVersion, files, oneScopePerScript);
                Log.WriteLine("Migrated up to version:".PadRight(30) + targetVersion);
            }
            else if (currentVersion > targetVersion)
            {
                direction = MigrationDirection.Down;
                MigrateDown(currentVersion, targetVersion, files, oneScopePerScript);
                Log.WriteLine("Migrated down to version:".PadRight(30) + targetVersion);
            }
            else
            {
                return;
            }

            // execute the post migration actions
            var postMigrationHooks = Program.Current.CommandRepository.Commands
                .Where(cmd => cmd is IPostMigrationHook)
                .Cast<IPostMigrationHook>()
                .Where(hook => hook.ShouldRun(direction));

            if (postMigrationHooks.Count() > 0)
            {
                Log.WriteLine("Executing post migration hooks...");

                foreach (var hook in postMigrationHooks)
                {
                    Log.WriteLine("  {0}", hook.CommandName);
                    hook.Log = Log;
                    hook.OnPostMigration(args, direction);
                }
            }
            else
            {
                Log.WriteLine("No post migration hooks were run.");
            }
        }

        /// <summary>
        /// Migrates the database up to the targeted version.
        /// </summary>
        /// <param name="currentVersion">The current version of the database.</param>
        /// <param name="targetVersion">The targeted version of the database.</param>
        /// <param name="files">All migration script files.</param>
        /// <param name="oneTransactionPerScript"></param>
        private void MigrateUp(long currentVersion, long targetVersion, IEnumerable<IMigrationScriptFile> files, bool oneTransactionPerScript)
        {
            var scripts = files.OrderBy(x => x.Version)
                .Where(x => x.Version > currentVersion && x.Version <= targetVersion)
                .Select(x => new KeyValuePair<IMigrationScriptFile, string>(x, x.Read().Setup));

            ExecuteMigrationScripts(scripts, UpdateSchemaVersionUp, oneTransactionPerScript);
        }

        /// <summary>
        /// Migrates the database down to the targeted version.
        /// </summary>
        /// <param name="currentVersion">The current version of the database.</param>
        /// <param name="targetVersion">The targeted version of the database.</param>
        /// <param name="files">All migration script files.</param>
        /// <param name="oneTransactionPerScript"></param>
        private void MigrateDown(long currentVersion, long targetVersion, IEnumerable<IMigrationScriptFile> files, bool oneTransactionPerScript)
        {
            var scripts = files.OrderByDescending(x => x.Version)
                .Where(x => x.Version <= currentVersion && x.Version > targetVersion)
                .Select(x => new KeyValuePair<IMigrationScriptFile, string>(x, x.Read().Teardown));

            ExecuteMigrationScripts(scripts, UpdateSchemaVersionDown, oneTransactionPerScript);
        }

        private void ExecuteMigrationScripts(IEnumerable<KeyValuePair<IMigrationScriptFile, string>> scripts, Action<DbTransaction, long> updateVersionAction, bool oneTransactionPerScript)
        {
            if (oneTransactionPerScript)
            {
                OneTransactionPerScript(scripts, updateVersionAction);
            }
            else
            {
                SingleTransaction(scripts, updateVersionAction);
            }
        }

        private void OneTransactionPerScript(IEnumerable<KeyValuePair<IMigrationScriptFile, string>> scripts, Action<DbTransaction, long> updateVersionAction)
        {
            Console.WriteLine("  Using One Transaction Per Script:");

            foreach (var script in scripts)
            {
                using (var tran = Database.BeginTransaction())
                {
                    try
                    {
                        ExecuteScript(updateVersionAction, script, tran);

                        tran.Commit();
                    }
                    catch (Exception ex)
                    {
                        tran.Rollback();

                        var filePath = script.Key?.FilePath ?? "NULL";
                        throw new MigrationException("Error executing migration script: " + filePath, filePath, ex);
                    }
                }
            }
        }

        private string ExecuteScript(Action<DbTransaction, long> updateVersionAction, KeyValuePair<IMigrationScriptFile, string> script, DbTransaction tran)
        {
            var filePath = script.Key?.FilePath ?? "NULL";

            Console.WriteLine(GetNow() + " - Start: " + filePath);

            Database.ExecuteScript(tran, script.Value);
            updateVersionAction(tran, script.Key.Version);

            Console.WriteLine(GetNow() + " - End: " + filePath);
            return filePath;
        }

        private void SingleTransaction(IEnumerable<KeyValuePair<IMigrationScriptFile, string>> scripts,
            Action<DbTransaction, long> updateVersionAction)
        {
            Console.WriteLine("  Using SingleTransaction");

            using (var tran = Database.BeginTransaction())
            {
                IMigrationScriptFile currentScript = null;
                try
                {
                    foreach (var script in scripts)
                    {
                        ExecuteScript(updateVersionAction, script, tran);
                    }

                    tran.Commit();
                }
                catch (Exception ex)
                {
                    tran.Rollback();

                    var filePath = (currentScript == null) ? "NULL" : currentScript.FilePath;
                    throw new MigrationException("Error executing migration script: " + filePath, filePath, ex);
                }
            }
        }

        private static string GetNow()
        {
            return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        }

        /// <summary>
        /// Updates the database with the version provided
        /// </summary>
        /// <param name="transaction">The transaction to execute the command in</param>
        /// <param name="version">The version to log</param>
        private static void UpdateSchemaVersionUp(DbTransaction transaction, long version)
        {
            const string sql = "INSERT INTO [schema_migrations] ([version]) VALUES ({0})";
            using (var cmd = transaction.CreateCommand())
            {
                cmd.CommandText = string.Format(sql, version);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Removes the provided version from the database log table.
        /// </summary>
        /// <param name="transaction">The transaction to execute the command in</param>
        /// <param name="version">The version to log</param>
        private static void UpdateSchemaVersionDown(DbTransaction transaction, long version)
        {
            const string sql = "DELETE FROM [schema_migrations] WHERE version = {0}";
            using (var cmd = transaction.CreateCommand())
            {
                cmd.CommandText = string.Format(sql, version);
                cmd.ExecuteNonQuery();
            }
        }
    }
}