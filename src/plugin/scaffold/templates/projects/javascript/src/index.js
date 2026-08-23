import { ActionResult, Plugin } from '@onelastleaf/plugin-sdk';
import { echo } from './echo.js';

await new Plugin('__OLL_PLUGIN_ID__', '0.1.0')
  .action('echo', 'Return the supplied arguments', async (_context, arguments_) =>
    ActionResult.string(echo(arguments_)))
  .run();
