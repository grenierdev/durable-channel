import { createFileRoute } from '@tanstack/react-router'
import { llmsIndex, textResponse } from '../lib/llms'

export const Route = createFileRoute('/llms.txt')({
  server: { handlers: { GET: () => textResponse(llmsIndex()) } },
})
